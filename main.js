const WebSocket = require('ws');
const { Client } = require('ssh2');
const fs = require('fs');
const path = require('path');
const jwt = require('jsonwebtoken');
const { connect, StringCodec } = require('nats');
const { v4: uuidv4 } = require('uuid');
require('dotenv').config();

// Session timeout mechanism with environment removal notification
const ENV_SESSION_DURATION = parseInt(process.env.ENV_SESSION_DURATION || '60000'); // 1 minute by default for testing
const ENV_CHECK_INTERVAL = 30000; // Check every 30 seconds

// Cooldown configuration
const USER_COOLDOWN_DURATION = parseInt(process.env.USER_COOLDOWN_DURATION || '300000'); // 5 minutes by default

// NATS connection configuration
const NATS_URL = process.env.NATS_URL || 'nats://localhost:4222';
let natsConnection = null;
const sc = StringCodec();

// Track list of VMs
let vmPool = [];
const userSessions = new Map();
const pendingVMRequests = new Map();
const pendingVMDeletions = new Map();
const terminatedUsers = new Map();

// Connect to NATS
async function connectToNATS() {
  try {
    natsConnection = await connect({ servers: NATS_URL });
    console.log(`Connected to NATS server at ${NATS_URL}`);
    
    // Subscribe to VM creation messages
    const subscription = natsConnection.subscribe('vm.created');
    console.log('Subscribed to vm.created channel');
    
    // Subscribe to VM creation error messages
    const errorSubscription = natsConnection.subscribe('vm.creation.error');
    console.log('Subscribed to vm.creation.error channel');
    
    // Subscribe to VM deletion responses - Add for Issue 1
    const deletionResponseSubscription = natsConnection.subscribe('vm.deletion.response');
    console.log('Subscribed to vm.deletion.response channel');
    
    // Process VM creation messages in a separate function
    processVMCreationMessages(subscription);
    
    // Process VM creation error messages in a separate function
    processVMCreationErrorMessages(errorSubscription);
    
    // Process VM deletion responses - Add for Issue 1
    processVMDeletionResponses(deletionResponseSubscription);
    
  } catch (error) {
    console.error('Error connecting to NATS:', error);
    // Retry connection after delay
    setTimeout(connectToNATS, 5000);
  }
}

// Process VM creation messages
async function processVMCreationMessages(subscription) {
  for await (const msg of subscription) {
    try {
      const vmData = JSON.parse(sc.decode(msg.data));
      console.log(`Received VM creation notification for: ${vmData.id}`);
      
      // Add the VM to our pool
      handleVMCreated(vmData);
    } catch (error) {
      console.error('Error processing vm.created message:', error);
    }
  }
}

// Process VM creation error messages
async function processVMCreationErrorMessages(subscription) {
  for await (const msg of subscription) {
    try {
      const errorData = JSON.parse(sc.decode(msg.data));
      console.log(`Received VM creation error for request ID: ${errorData.request_id}`);
      
      // Handle VM creation error
      handleVMCreationError(errorData);
    } catch (error) {
      console.error('Error processing vm.creation.error message:', error);
    }
  }
}

// Process VM deletion responses - Add for Issue 1
async function processVMDeletionResponses(subscription) {
  for await (const msg of subscription) {
    try {
      const response = JSON.parse(sc.decode(msg.data));
      console.log(`Received VM deletion response for request ID: ${response.request_id}, status: ${response.status}`);
      
      // Check if we have a pending deletion request with this ID
      if (pendingVMDeletions.has(response.request_id)) {
        const deletion = pendingVMDeletions.get(response.request_id);
        
        if (response.status === 'success') {
          console.log(`VM ${deletion.vmId} successfully deleted by deletion service`);
        } else {
          console.error(`VM deletion failed for ${deletion.vmId}: ${response.error || 'unknown error'}`);
        }
        
        // Cleanup the pending deletion request
        pendingVMDeletions.delete(response.request_id);
      } else {
        console.log(`Received deletion response for unknown request: ${response.request_id}`);
      }
    } catch (error) {
      console.error('Error processing vm.deletion.response message:', error);
    }
  }
}

// Handle VM created message
function handleVMCreated(vmData) {
  // Add VM to pool
  vmPool.push(vmData);
  
  // Check if we have pending requests for this VM
  const userId = vmData.userId;
  
  if (userId && pendingVMRequests.has(userId)) {
    const pendingRequest = pendingVMRequests.get(userId);
    pendingVMRequests.delete(userId);
    
    // Create user session assignment
    userSessions.set(userId, {
      vmId: vmData.id,
      vmIp: vmData.ip,
      assignedAt: Date.now(),
      terminals: new Set(pendingRequest.terminals)
    });
    
    console.log(`VM ${vmData.id} (${vmData.ip}) is now ready for user ${userId}`);
    
    // Notify clients that VM is ready
    wss.clients.forEach((ws) => {
      if (ws.userId === userId) {
        try {
          ws.send(JSON.stringify({
            type: 'vm_ready',
            message: 'Your environment is ready',
            vmId: vmData.id
          }));
          
          // If this client has a pending SSH connection, establish it now
          if (pendingRequest.pendingConnections.has(ws.terminalId)) {
            establishSSHConnection(ws, vmData.ip);
            pendingRequest.pendingConnections.delete(ws.terminalId);
          }
        } catch (error) {
          console.error('Error sending VM ready message:', error);
        }
      }
    });
  }
}

// Handle VM creation error messages
function handleVMCreationError(errorData) {
  // Try to find the affected user session
  const requestId = errorData.request_id;
  let affectedUserId = null;
  
  // Check if we can find a user ID with a pending request
  pendingVMRequests.forEach((request, userId) => {
    // If we find a match, notify clients
    affectedUserId = userId;
  });
  
  if (affectedUserId) {
    const pendingRequest = pendingVMRequests.get(affectedUserId);
    
    // Notify all waiting terminals for this user
    pendingRequest.pendingConnections.forEach((ws) => {
      try {
        ws.send(JSON.stringify({
          type: 'error',
          message: `VM creation failed: ${errorData.error}`,
          details: errorData
        }));
      } catch (error) {
        console.error('Error sending creation error message:', error);
      }
    });
    
    // Clean up the pending request
    pendingVMRequests.delete(affectedUserId);
    console.log(`Cleaned up pending VM request for user ${affectedUserId} due to creation error`);
  } else {
    console.log(`Received VM creation error but couldn't match to pending request: ${JSON.stringify(errorData)}`);
  }
}

// Establish SSH connection
function establishSSHConnection(ws, vmIp) {
  // Configuration
  const MAX_RETRIES = 10;
  const INITIAL_RETRY_DELAY = 3000; // 3 seconds
  const MAX_RETRY_DELAY = 15000; // 15 seconds
  
  let currentRetry = 0;
  let retryDelay = INITIAL_RETRY_DELAY;

  // Function to attempt SSH connection with retries
  function attemptSSHConnection() {
    const ssh = new Client();
    let stream = null;
    
    // Store SSH connection on websocket
    ws.ssh = ssh;
    
    // Connect to the VM
    console.log(`SSH connection attempt ${currentRetry + 1}/${MAX_RETRIES} for terminal ${ws.terminalId} to VM at ${vmIp}`);
    
    // Log SSH connection details (without sensitive info)
    console.log(`SSH connection details: host=${vmIp}, port=${parseInt(process.env.SSH_PORT || '22')}, username=${process.env.VM_USER}`);
    
    // Add timeout for connection
    const sshTimeout = setTimeout(() => {
      console.error(`SSH connection timed out for ${ws.terminalId} to ${vmIp}`);
      ssh.end();
      handleConnectionFailure(new Error("SSH connection timed out"), ssh);
    }, 30000); // 30 second timeout
    
    ssh.on('ready', () => {
      console.log(`SSH connection established to ${vmIp} for terminal ${ws.terminalId}`);
      clearTimeout(sshTimeout);
      
      ws.send(JSON.stringify({ type: 'connected', message: 'SSH connection established' }));
      
      // Request a PTY (pseudo-terminal) with specific size
      ssh.shell({ 
            term: 'xterm-256color',
            rows: 24,
            cols: 120, // Increased columns
            modes: {
                ECHO: 1,          // Enable echo
                ICANON: 1,        // Enable canonical mode (line buffering)
                ISIG: 1           // Enable signals
            }
        }, (err, shellStream) => {
        if (err) {
          console.error(`Failed to open shell on ${vmIp}: ${err.message}`);
          ws.send(JSON.stringify({ type: 'error', message: `Failed to open shell: ${err.message}` }));
          return;
        }
        
        stream = shellStream;
        ws.stream = stream;
        
        // Add a buffer for the initial output to prevent duplicate prompts
        let initialOutputBuffer = '';
        let initialOutputComplete = false;
        
        // Send raw SSH output directly to client
        stream.on('data', (data) => {
          const dataStr = data.toString('utf-8');
          
          // Only buffer the first prompt
          if (!initialOutputComplete) {
            initialOutputBuffer += dataStr;
            
            // If we detect a shell prompt, consider initialization complete
            if (initialOutputBuffer.includes('$') || initialOutputBuffer.includes('#') || initialOutputBuffer.includes('>')) {
              initialOutputComplete = true;
              ws.send(JSON.stringify({ 
                type: 'data', 
                data: initialOutputBuffer
              }));
            }
          } else {
            ws.send(JSON.stringify({ 
              type: 'data', 
              data: dataStr
            }));
          }
        });
        
        stream.stderr.on('data', (data) => {
          const errorText = data.toString('utf-8');
          console.error(`SSH stderr from ${vmIp}: ${errorText}`);
          ws.send(JSON.stringify({ 
            type: 'data', 
            data: errorText
          }));
        });
        
        stream.on('close', () => {
          console.log(`SSH stream closed for ${vmIp} (terminal ${ws.terminalId})`);
          ws.send(JSON.stringify({ type: 'closed', message: 'SSH connection closed' }));
          ssh.end();
        });
      });
    });
    
    ssh.on('error', (err) => {
      clearTimeout(sshTimeout);
      handleConnectionFailure(err, ssh);
    });
    
    ssh.on('close', () => {
      console.log(`SSH connection closed for ${vmIp} (terminal ${ws.terminalId})`);
    });
    
    ssh.on('end', () => {
      console.log(`SSH connection ended for ${vmIp} (terminal ${ws.terminalId})`);
    });
    
    /*ssh.on('handshake', (negotiated) => {
      //console.log(`SSH handshake completed for ${vmIp} with algorithms:`, negotiated);
      console.log(`SSH handshake completed for ${vmIp}`);
    });*/
    
    try {
      ssh.connect({
        host: vmIp,
        port: parseInt(process.env.SSH_PORT || '22'),
        username: process.env.VM_USER,
        privateKey: fs.readFileSync(process.env.SSH_PRIVATE_KEY_PATH),
        debug: process.env.SSH_DEBUG === 'true', // Enable debug output when env var is set
        readyTimeout: 30000, // 30 second timeout
        // Add connection parameters that might help with stability
        keepaliveInterval: 5000, // Send keepalive every 5 seconds
        keepaliveCountMax: 5,    // Allow 5 missed keepalives before disconnecting
        algorithms: {
          // Explicitly specify only the most common algorithms to avoid compatibility issues
          kex: ['ecdh-sha2-nistp256', 'ecdh-sha2-nistp384', 'ecdh-sha2-nistp521', 'diffie-hellman-group-exchange-sha256'],
          cipher: ['aes128-ctr', 'aes192-ctr', 'aes256-ctr'],
          serverHostKey: ['ssh-rsa', 'ecdsa-sha2-nistp256', 'ssh-ed25519'],
          hmac: ['hmac-sha2-256', 'hmac-sha2-512']
        }
      });
    } catch (err) {
      // Handle any synchronous exceptions during connection setup
      clearTimeout(sshTimeout);
      handleConnectionFailure(err, ssh);
    }
  }
  
  // Function to handle connection failures and retry if appropriate
  function handleConnectionFailure(err, ssh) {
    const errorDetails = {
      message: err.message,
      code: err.code,
      level: err.level,
      syscall: err.syscall,
      errno: err.errno
    };
    
    console.error(`SSH connection error for ${vmIp} (terminal ${ws.terminalId}): ${err.message}`);
    console.error(`Error details:`, errorDetails);
    
    // Close failed connection
    try {
      ssh.end();
    } catch (e) {
      // Ignore errors when ending an already failed connection
    }
    
    // Check if we should retry
    if (currentRetry < MAX_RETRIES - 1) {
      currentRetry++;
      
      // Notify client about retry
      ws.send(JSON.stringify({ 
        type: 'connecting', 
        message: `SSH connection failed. Retrying (${currentRetry}/${MAX_RETRIES}) in ${retryDelay/1000} seconds...`,
        attempt: currentRetry,
        maxAttempts: MAX_RETRIES,
        error: err.message
      }));
      
      // Exponential backoff for next retry
      setTimeout(() => {
        attemptSSHConnection();
        
        // Increase delay for next retry (with a cap)
        retryDelay = Math.min(retryDelay * 1.5, MAX_RETRY_DELAY);
      }, retryDelay);
    } else {
      // No more retries, send final error to client
      ws.send(JSON.stringify({ 
        type: 'error', 
        message: `SSH connection failed after ${MAX_RETRIES} attempts: ${err.message}`,
        details: errorDetails
      }));
      
      ws.close();
    }
  }
  
  // Start first attempt
  attemptSSHConnection();
}

// Function to trigger environment timeouts
function checkEnvironmentTimeout() {
  const now = Date.now();
  console.log(`Running environment timeout check at ${new Date(now).toISOString()}`);
  
  userSessions.forEach((assignment, userId) => {
    const sessionDuration = now - assignment.assignedAt;
    
    if (sessionDuration > ENV_SESSION_DURATION) {
      console.log(`TIMEOUT TRIGGERED for user ${userId}, session duration: ${sessionDuration}ms, threshold: ${ENV_SESSION_DURATION}ms`);
      
      // Find the VM in the pool
      const vmToDelete = vmPool.find(vm => vm.id === assignment.vmId);
      
      if (vmToDelete) {
        console.log(`Found VM to delete: ${vmToDelete.id}`);
        
        // First, close all WebSocket connections for this client
        let closedConnections = 0;
        wss.clients.forEach((ws) => {
          if (ws.userId === userId) {
            try {
              // Try to send a termination message, but don't worry if it fails
              if (ws.readyState === WebSocket.OPEN) {
                try {
                  ws.send(JSON.stringify({
                    type: 'environment_terminated',
                    message: 'Environment session expired.',
                    vmId: vmToDelete.id
                  }));
                } catch (err) {
                  // Ignore send errors
                }
              }
              
              // Force close the connection
              ws.terminate(); // Hard close, doesn't wait for close frames
              closedConnections++;
            } catch (error) {
              console.error(`Error terminating WebSocket for ${ws.terminalId}:`, error);
            }
          }
        });
        
        console.log(`Forcibly closed ${closedConnections} connections for user ${userId}`);
        
        // Request VM deletion via NATS
        requestVMDeletion(vmToDelete.id, userId, 'session_timeout');
        
        // Add user to cooldown list
        addUserToCooldown(userId);
      }
      
      // Release the VM from user sessions
      releaseVM(userId);
    }
  });
}

// Function to add a user to the cooldown list
function addUserToCooldown(userId) {
  const expiresAt = Date.now() + USER_COOLDOWN_DURATION;
  terminatedUsers.set(userId, expiresAt);
  console.log(`Added user ${userId} to cooldown until ${new Date(expiresAt).toISOString()}`);
}

// Function to check if a user is in cooldown
function isUserInCooldown(userId) {
  if (terminatedUsers.has(userId)) {
    const expiresAt = terminatedUsers.get(userId);
    const now = Date.now();
    
    if (now < expiresAt) {
      // Still in cooldown
      const remainingTime = Math.ceil((expiresAt - now) / 1000);
      console.log(`User ${userId} is in cooldown for ${remainingTime} more seconds`);
      return true;
    } else {
      // Cooldown expired, remove from map
      terminatedUsers.delete(userId);
      console.log(`User ${userId} cooldown expired, removed from restrictions`);
      return false;
    }
  }
  
  return false;
}

// Modified: Function to request VM deletion via NATS with request-reply pattern
async function requestVMDeletion(vmId, userId, reason) {
  if (!natsConnection) {
    console.error('Cannot request VM deletion: NATS connection not available');
    return;
  }
  
  try {
    // Generate a unique request ID for tracking
    const requestId = uuidv4();
    
    // Store deletion request in pending map
    pendingVMDeletions.set(requestId, {
      vmId,
      userId,
      reason,
      requestedAt: Date.now()
    });
    
    // Use request-reply pattern instead of simple publish - Issue 1 fix
    const reply = await natsConnection.request(
      'vm.delete',
      sc.encode(JSON.stringify({
        requestId: requestId,
        vmId: vmId,
        userId: userId,
        reason: reason,
        timestamp: Date.now()
      })),
      { timeout: 10000 } // 10 second timeout for reply
    );
    
    // Process the immediate acknowledgment
    const response = JSON.parse(sc.decode(reply.data));
    console.log(`VM deletion request for ${vmId} acknowledged: ${JSON.stringify(response)}`);
    
    // Remove VM from local pool
    const vmIndex = vmPool.findIndex(vm => vm.id === vmId);
    if (vmIndex !== -1) {
      vmPool.splice(vmIndex, 1);
      console.log(`Removed VM ${vmId} from local pool`);
    }
  } catch (error) {
    console.error(`Error requesting VM deletion for ${vmId}:`, error);
    
    // Check if we need to clean up from pendingVMDeletions
    // (We'll find by vmId since requestId might not be accessible in this catch block)
    for (const [requestId, details] of pendingVMDeletions.entries()) {
      if (details.vmId === vmId) {
        pendingVMDeletions.delete(requestId);
        console.log(`Cleaned up pending VM deletion for ${vmId} due to request error`);
      }
    }
  }
}

// Periodically clean up expired cooldowns - Part of Issue 2 solution
setInterval(() => {
  const now = Date.now();
  let expiredCount = 0;
  
  terminatedUsers.forEach((expiresAt, userId) => {
    if (now >= expiresAt) {
      terminatedUsers.delete(userId);
      expiredCount++;
    }
  });
  
  if (expiredCount > 0) {
    console.log(`Cleaned up ${expiredCount} expired user cooldowns`);
  }
}, 60000); // Check every minute

// Start the environment timeout check interval
const environmentTimeoutInterval = setInterval(checkEnvironmentTimeout, ENV_CHECK_INTERVAL);

// Optional: Cleanup interval on server shutdown
process.on('SIGINT', async () => {
  clearInterval(environmentTimeoutInterval);
  console.log('Environment timeout checker stopped');
  
  if (natsConnection) {
    await natsConnection.drain();
    console.log('NATS connection drained');
  }
  
  process.exit(0);
});

// Function to assign a VM to a user session
async function assignVMToUser(userId, terminalId, ws) {
    // Check for existing assignment first
    if (userSessions.has(userId)) {
        const existingAssignment = userSessions.get(userId);
        
        // Add this terminal to the existing session
        existingAssignment.terminals.add(terminalId);
        
        // If VM is already ready, return the assignment
        // Otherwise, add this terminal to pending connections
        if (vmReady[existingAssignment.vmId]) {
            return existingAssignment;
        } else {
            // Add to pending connections for later
            if (pendingVMRequests.has(userId)) {
                const pendingRequest = pendingVMRequests.get(userId);
                pendingRequest.pendingConnections.set(terminalId, ws);
            }
            
            // Let client know we're still waiting for VM
            ws.send(JSON.stringify({
                type: 'vm_creating',
                message: 'Your environment is being created. Please wait...'
            }));
            
            return null;
        }
    }
  
    if (isUserInCooldown(userId)) {
        // Calculate remaining cooldown time
        const remainingSeconds = Math.ceil((terminatedUsers.get(userId) - Date.now()) / 1000);
        
        // Notify client about cooldown
        ws.send(JSON.stringify({
        type: 'error',
        message: `Your previous session recently expired. Please wait ${remainingSeconds} seconds before starting a new session.`
        }));
        
        return null;
    }
  
  // Find available VM in current pool
  const availableVM = vmPool.find(vm => !vm.busy);
  
  if (availableVM) {
    // Mark VM as busy
    availableVM.busy = true;
    availableVM.userId = userId;
    
    // Create user session
    userSessions.set(userId, {
      vmId: availableVM.id,
      vmIp: availableVM.ip,
      assignedAt: Date.now(),
      terminals: new Set([terminalId])
    });
    
    console.log(`Assigned existing VM ${availableVM.id} (${availableVM.ip}) to user ${userId}`);
    return userSessions.get(userId);
  }
  
  // No existing VM available, request new one via NATS
  if (!natsConnection) {
    console.error('Cannot request VM creation: NATS connection not available');
    return null;
  }
  
  // Create a pending request entry
  pendingVMRequests.set(userId, {
    requestedAt: Date.now(),
    terminals: new Set([terminalId]),
    pendingConnections: new Map([[terminalId, ws]])
  });
  
  // Notify client that VM is being created
  ws.send(JSON.stringify({
    type: 'vm_creating',
    message: 'Your environment is being created. Please wait...'
  }));
  
  console.log(`No VM available, will request one via NATS for user ${userId}`);
  console.log(`NATS connection status:`, natsConnection ? 'Connected' : 'Not connected');  

  // Request VM creation
  try {
    // Generate a unique request ID for this VM creation
    const requestId = uuidv4();
    
    // Send request-reply message
    const reply = await natsConnection.request('vm.create', sc.encode(JSON.stringify({
      requestId: requestId,
      userId: userId,
      requestedAt: Date.now()
    })), { timeout: 10000 });
    
    const response = JSON.parse(sc.decode(reply.data));
    console.log(`VM creation request acknowledged: ${JSON.stringify(response)}`);
    
    // The actual VM details will come later on the vm.created subscription
    return null;
  } catch (error) {
    console.error(`Error requesting VM creation for user ${userId}:`, error);
    
    // Clean up pending request on error
    pendingVMRequests.delete(userId);
    
    // Notify client of error
    ws.send(JSON.stringify({
      type: 'error',
      message: 'Failed to create environment. Please try again later.'
    }));
    
    return null;
  }
}

// Function to release a VM when user session ends
function releaseVM(userId) {
  const assignment = userSessions.get(userId);
  if (!assignment) return;
  
  // Find the VM in the pool
  const vm = vmPool.find(vm => vm.id === assignment.vmId);
  if (vm) {
    // If VM is in our pool, mark it as no longer busy
    vm.busy = false;
    vm.userId = null;
    console.log(`Released VM ${vm.id} (${vm.ip}) from user ${userId}`);
  }
  
  // Remove from sessions map
  userSessions.delete(userId);
}

// Create WebSocket server
const wss = new WebSocket.Server({ port: process.env.WS_PORT || 8081 });

wss.on('connection', (ws, req) => {
  // Parse query parameters
  const urlParams = new URL(`http://localhost${req.url}`).searchParams;
  
  const userId = urlParams.get('userid');
  if (!userId) {
    ws.send(JSON.stringify({ type: 'error', message: 'Authentication required' }));
    ws.close();
    return;
  }
  // Terminal ID is unique per terminal tab
  const terminalId = urlParams.get('terminalid') || `terminal_${Date.now()}_${Math.random().toString(36).substring(2, 15)}`;
  
  console.log(`Terminal ${terminalId} connected from user ${userId}`);
  
  // Store session info on the websocket object
  ws.userId = userId;
  ws.terminalId = terminalId;
  
  // Handle messages from client
  ws.on('message', async (message) => {
    try {
        const data = JSON.parse(message);
      
        // Handle authentication
        if (data.type === 'auth') {
          try {
              // Check if this is a reconnection attempt from the same user with an active session
              const hasActiveSession = userSessions.has(userId);
              
              if (hasActiveSession) {
                  console.log(`User ${userId} has an active session, allowing reconnection`);
                  
                  // Get the existing session
                  const existingSession = userSessions.get(userId);
                  
                  // Add this terminal to the existing session
                  existingSession.terminals.add(terminalId);
                  
                  // IMPORTANT: Notify the client that VM is ready before establishing SSH connection
                  // This allows the client to properly initialize the terminal
                  ws.send(JSON.stringify({
                      type: 'vm_ready',
                      message: 'Your environment is ready',
                      vmId: existingSession.vmId,
                      vmIp: existingSession.vmIp
                  }));
                  
                  // Wait a brief moment then establish SSH connection
                  setTimeout(() => {
                      // Connect to the assigned VM
                      establishSSHConnection(ws, existingSession.vmIp);
                  }, 500);
                  return;
              }
              
              // For users without an active session, check for cooldown
              if (isUserInCooldown(userId)) {
                  // Calculate remaining cooldown time
                  const remainingSeconds = Math.ceil((terminatedUsers.get(userId) - Date.now()) / 1000);
                  
                  // Notify client about cooldown
                  ws.send(JSON.stringify({
                      type: 'error',
                      message: `Your previous session recently expired. Please wait ${remainingSeconds} seconds before starting a new session.`
                  }));
                  
                  return;
              }
              
              // Remove this redundant check - you've already checked hasActiveSession above
              // if (userSessions.has(userId)) {
              //     ws.send(JSON.stringify({
              //     type: 'error',
              //     message: 'You already have an active VM session. Please use that session or terminate it first.'
              //     }));
              //     return;
              // }
      
              // Assign a VM for this user
              const assignment = await assignVMToUser(userId, terminalId, ws);
      
              if (!assignment) {
                  // This is not an error, just means VM is being created
                  // Client has already been notified via ws.send in assignVMToUser
                  return;
              }
              
              // When a VM is newly assigned, notify the client first
              ws.send(JSON.stringify({
                  type: 'vm_ready',
                  message: 'Your environment is ready',
                  vmId: assignment.vmId,
                  vmIp: assignment.vmIp
              }));
              
              // Then connect to the assigned VM after a short delay
              setTimeout(() => {
                  establishSSHConnection(ws, assignment.vmIp);
              }, 500);
          } catch (err) {
              ws.send(JSON.stringify({ type: 'error', message: `Authentication failed: ${err.message}` }));
              ws.close();
          }
        }
      
        else if (data.type === 'establish_ssh') {
            const vmId = data.vmId;
            const session = userSessions.get(userId);
            
            if (session && session.vmIp) {
                console.log(`Establishing SSH connection for user ${userId} to VM ${vmId}`);
                establishSSHConnection(ws, session.vmIp);
            } else {
                ws.send(JSON.stringify({ 
                    type: 'error', 
                    message: 'Cannot establish SSH connection: No VM assigned' 
                }));
            }
        }

        else if (data.type === 'connect_ssh') {
            const vmId = data.vmId;
            // Find VM IP based on vmId
            const vm = vmPool.find(vm => vm.id === vmId);
            
            if (vm && vm.ip) {
                console.log(`Establishing SSH connection for user ${userId} to VM ${vmId} (${vm.ip})`);
                establishSSHConnection(ws, vm.ip);
            } else {
                ws.send(JSON.stringify({ 
                    type: 'error', 
                    message: 'Cannot establish SSH connection: VM not found' 
                }));
            }
        }

        // Handle raw terminal data
        else if (data.type === 'data' && ws.stream) {
            ws.stream.write(data.data);
        }
        
        // Handle terminal resize events
        else if (data.type === 'resize' && ws.stream) {
            ws.stream.setWindow(data.rows, data.cols, 0, 0);
        }
    } catch (err) {
      console.error('Error processing message:', err);
      ws.send(JSON.stringify({ type: 'error', message: `Message processing error: ${err.message}` }));
    }
  });
  
  // Handle client disconnect
  ws.on('close', () => {
    console.log(`Terminal ${terminalId} from user ${userId} disconnected`);
    
    // Close SSH connection if exists
    if (ws.stream) {
      try {
        ws.stream.close();
      } catch (e) {
        console.error('Error closing stream:', e);
      }
    }
    
    if (ws.ssh) {
      try {
        ws.ssh.end();
      } catch (e) {
        console.error('Error ending SSH connection:', e);
      }
    }
    
    // Check if there's a pending VM request for this user
    if (pendingVMRequests.has(userId)) {
      const pendingRequest = pendingVMRequests.get(userId);
      pendingRequest.terminals.delete(terminalId);
      pendingRequest.pendingConnections.delete(terminalId);
      
      // If no more terminals are waiting, cancel the VM request
      if (pendingRequest.terminals.size === 0) {
        console.log(`No more terminals waiting for VM, canceling request for user ${userId}`);
        pendingVMRequests.delete(userId);
        
        // Optionally notify VM service to cancel creation if possible
        if (natsConnection) {
          try {
            // Use request-reply pattern for VM cancel as well
            const requestId = uuidv4();
            natsConnection.request(
              'vm.cancel', 
              sc.encode(JSON.stringify({
                requestId: requestId,
                userId: userId,
                timestamp: Date.now()
              })), 
              { timeout: 5000 }
            ).then(reply => {
              const response = JSON.parse(sc.decode(reply.data));
              console.log(`VM cancel request acknowledged: ${JSON.stringify(response)}`);
            }).catch(err => {
              console.error(`Error in VM cancel request:`, err);
            });
          } catch (e) {
            console.error('Error initiating cancel message:', e);
          }
        }
      }
      return;
    }
    
    // If not pending, check active sessions
    const assignment = userSessions.get(userId);
    if (assignment) {
      assignment.terminals.delete(terminalId);
      
      // Only release the VM if no terminals from this user are connected
      if (assignment.terminals.size === 0) {
        console.log(`No more terminals from user ${userId}, releasing VM ${assignment.vmId}`);
        releaseVM(userId);
      } else {
        console.log(`User ${userId} still has ${assignment.terminals.size} active terminals`);
      }
    }
  });
});

// Periodically check for abandoned pending requests and deletion requests
setInterval(() => {
  const now = Date.now();
  
  // Check for abandoned pending VM requests
  pendingVMRequests.forEach((request, userId) => {
    // If a pending request is older than 10 minutes, consider it abandoned
    if (now - request.requestedAt > 600000) { // 10 minutes
      console.log(`Pending VM request for user ${userId} timed out`);
      
      // Notify all waiting terminals
      request.pendingConnections.forEach((ws) => {
        try {
          ws.send(JSON.stringify({
            type: 'error',
            message: 'Environment creation timed out. Please try again.'
          }));
        } catch (e) {
          console.error('Error sending timeout message:', e);
        }
      });
      
      // Clean up the pending request
      pendingVMRequests.delete(userId);
      
      // Try to cancel VM creation using request-reply pattern
      if (natsConnection) {
        try {
          const requestId = uuidv4();
          natsConnection.request('vm.cancel', sc.encode(JSON.stringify({
            requestId: requestId,
            userId: userId,
            reason: 'timeout',
            timestamp: Date.now()
          })), { timeout: 5000 }).then(reply => {
            const response = JSON.parse(sc.decode(reply.data));
            console.log(`VM cancel request for abandoned request acknowledged: ${JSON.stringify(response)}`);
          }).catch(err => {
            console.error(`Error in VM cancel request for abandoned request:`, err);
          });
        } catch (e) {
          console.error('Error initiating cancel message for abandoned request:', e);
        }
      }
    }
  });
  
  // Check for stuck VM deletion requests
  pendingVMDeletions.forEach((deletion, requestId) => {
    // If a deletion request is older than 5 minutes, consider it stuck
    if (now - deletion.requestedAt > 300000) { // 5 minutes
      console.log(`VM deletion request ${requestId} for VM ${deletion.vmId} has been pending for over 5 minutes`);
      
      // Retry the deletion with a new request ID
      if (natsConnection) {
        try {
          const newRequestId = uuidv4();
          console.log(`Retrying deletion with new request ID: ${newRequestId}`);
          
          natsConnection.request('vm.delete', sc.encode(JSON.stringify({
            requestId: newRequestId,
            originalRequestId: requestId,
            vmId: deletion.vmId,
            userId: deletion.userId,
            reason: `${deletion.reason}_retry`,
            timestamp: Date.now(),
            isRetry: true
          })), { timeout: 10000 }).then(reply => {
            const response = JSON.parse(sc.decode(reply.data));
            console.log(`VM deletion retry acknowledged: ${JSON.stringify(response)}`);
            
            // Track this as a new deletion request
            pendingVMDeletions.set(newRequestId, {
              ...deletion,
              requestedAt: Date.now(),
              isRetry: true,
              originalRequestId: requestId
            });
            
          }).catch(err => {
            console.error(`Error in VM deletion retry:`, err);
          });
          
          // Remove the original request regardless of retry outcome
          pendingVMDeletions.delete(requestId);
          
        } catch (e) {
          console.error(`Error initiating deletion retry for stuck request ${requestId}:`, e);
        }
      }
    }
  });
}, 60000); // Check every minute

// Periodically log system status
setInterval(() => {
  const now = Date.now();
  
  console.log(`\n--- System Status at ${new Date(now).toISOString()} ---`);
  console.log(`Active VM pool: ${vmPool.length} VMs`);
  console.log(`Active user sessions: ${userSessions.size}`);
  console.log(`Pending VM requests: ${pendingVMRequests.size}`);
  console.log(`Pending VM deletions: ${pendingVMDeletions.size}`);
  console.log(`Users in cooldown: ${terminatedUsers.size}`);
  
  // Detailed VM Pool Stats
  if (vmPool.length > 0) {
    console.log(`\nVM Pool Details:`);
    vmPool.forEach(vm => {
      console.log(`- VM ${vm.id} (${vm.ip}): ${vm.busy ? 'Busy' : 'Available'}${vm.userId ? ` (used by ${vm.userId})` : ''}`);
    });
  }
  
  // User sessions with their VMs
  if (userSessions.size > 0) {
    console.log(`\nUser Sessions:`);
    userSessions.forEach((session, userId) => {
      const sessionDuration = Math.floor((now - session.assignedAt) / 1000);
      const timeLeft = Math.floor((ENV_SESSION_DURATION - (now - session.assignedAt)) / 1000);
      
      console.log(`- User ${userId}: VM ${session.vmId} (${session.vmIp})`);
      console.log(`  Terminals: ${session.terminals.size}, Duration: ${sessionDuration}s, Time left: ${timeLeft}s`);
    });
  }
  
  // Users in cooldown
  if (terminatedUsers.size > 0) {
    console.log(`\nUsers in Cooldown:`);
    terminatedUsers.forEach((expiresAt, userId) => {
      const cooldownLeft = Math.floor((expiresAt - now) / 1000);
      console.log(`- User ${userId}: ${cooldownLeft}s remaining`);
    });
  }
  
  console.log(`---------------------------------------------\n`);
}, 300000); // Log status every 5 minutes

const port = process.env.WS_PORT || 8081;
console.log(`WebSocket server running on port ${port}`);

// Log initial configuration
console.log(`Server configuration:
- Environment session duration: ${ENV_SESSION_DURATION}ms (${ENV_SESSION_DURATION/60000} minutes)
- User cooldown duration: ${USER_COOLDOWN_DURATION}ms (${USER_COOLDOWN_DURATION/60000} minutes)
- Environment check interval: ${ENV_CHECK_INTERVAL}ms
- NATS server: ${NATS_URL}
`);

// Connect to NATS and start the server
connectToNATS().catch(error => {
  console.error('Failed to initialize NATS connection:', error);
});