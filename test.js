// ws-server.js - WebSocket server with browser-based VM allocation using NATS
const WebSocket = require('ws');
const { Client } = require('ssh2');
const fs = require('fs');
const path = require('path');
const jwt = require('jsonwebtoken');
const { connect, StringCodec } = require('nats');
const { v4: uuidv4 } = require('uuid'); // Add this dependency for request IDs
require('dotenv').config();

// Add a new session timeout mechanism with environment removal notification
//const ENV_SESSION_DURATION = parseInt(process.env.ENV_SESSION_DURATION || '14400000'); // 4 hours by default
const ENV_SESSION_DURATION = parseInt(process.env.ENV_SESSION_DURATION || '60000'); // 1 minute by default for testing

const ENV_CHECK_INTERVAL = 30000; // Check every 30 seconds

// Cooldown configuration - Add this for Issue 2
const BROWSER_COOLDOWN_DURATION = parseInt(process.env.BROWSER_COOLDOWN_DURATION || '300000'); // 5 minutes by default
const terminatedBrowsers = new Map(); // Map to track browsers in cooldown period

// NATS connection configuration
const NATS_URL = process.env.NATS_URL || 'nats://localhost:4222';
let natsConnection = null;
const sc = StringCodec();

// In-memory VM pool
let vmPool = [];

// Map to track browser sessions and their assigned VMs
const browserSessions = new Map();
// Map to track pending VM creation requests
const pendingVMRequests = new Map();
// Map to track pending VM deletion requests - Add for Issue 1
const pendingVMDeletions = new Map();

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
  const browserId = vmData.browserId;
  
  if (browserId && pendingVMRequests.has(browserId)) {
    const pendingRequest = pendingVMRequests.get(browserId);
    pendingVMRequests.delete(browserId);
    
    // Create browser session assignment
    browserSessions.set(browserId, {
      vmId: vmData.id,
      vmIp: vmData.ip,
      assignedAt: Date.now(),
      terminals: new Set(pendingRequest.terminals)
    });
    
    console.log(`VM ${vmData.id} (${vmData.ip}) is now ready for browser ${browserId}`);
    
    // Notify clients that VM is ready
    wss.clients.forEach((ws) => {
      if (ws.browserId === browserId) {
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
  // Try to find the affected browser session
  const requestId = errorData.request_id;
  let affectedBrowserId = null;
  
  // Check if we can find a browser ID with a pending request
  pendingVMRequests.forEach((request, browserId) => {
    // If we find a match, notify clients
    affectedBrowserId = browserId;
  });
  
  if (affectedBrowserId) {
    const pendingRequest = pendingVMRequests.get(affectedBrowserId);
    
    // Notify all waiting terminals for this browser
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
    pendingVMRequests.delete(affectedBrowserId);
    console.log(`Cleaned up pending VM request for browser ${affectedBrowserId} due to creation error`);
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
        cols: 80
      }, (err, shellStream) => {
        if (err) {
          console.error(`Failed to open shell on ${vmIp}: ${err.message}`);
          ws.send(JSON.stringify({ type: 'error', message: `Failed to open shell: ${err.message}` }));
          return;
        }
        
        stream = shellStream;
        ws.stream = stream;
        
        // Send raw SSH output directly to client
        stream.on('data', (data) => {
          ws.send(JSON.stringify({ 
            type: 'data', 
            data: data.toString('utf-8')
          }));
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
  
  browserSessions.forEach((assignment, browserId) => {
    // Check if the environment has exceeded its max duration
    const sessionDuration = now - assignment.assignedAt;
    
    if (sessionDuration > ENV_SESSION_DURATION) {
      console.log(`TIMEOUT TRIGGERED for browser ${browserId}, session duration: ${sessionDuration}ms, threshold: ${ENV_SESSION_DURATION}ms`);
      
      // Find the VM in the pool
      const vmToDelete = vmPool.find(vm => vm.id === assignment.vmId);
      
      if (vmToDelete) {
        console.log(`Found VM to delete: ${vmToDelete.id}`);
        
        // First, close all WebSocket connections for this browser
        let closedConnections = 0;
        wss.clients.forEach((ws) => {
          if (ws.browserId === browserId) {
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
        
        console.log(`Forcibly closed ${closedConnections} connections for browser ${browserId}`);
        
        // Request VM deletion via NATS
        requestVMDeletion(vmToDelete.id, browserId, 'session_timeout');
        
        // Add browser to cooldown list - Issue 2 solution
        addBrowserToCooldown(browserId);
      }
      
      // Release the VM from browser sessions
      releaseVM(browserId);
    }
  });
}

// Function to add a browser to the cooldown list - Part of Issue 2 solution
function addBrowserToCooldown(browserId) {
  const expiresAt = Date.now() + BROWSER_COOLDOWN_DURATION;
  terminatedBrowsers.set(browserId, expiresAt);
  console.log(`Added browser ${browserId} to cooldown until ${new Date(expiresAt).toISOString()}`);
}

// Function to check if a browser is in cooldown - Part of Issue 2 solution
function isBrowserInCooldown(browserId) {
  if (terminatedBrowsers.has(browserId)) {
    const expiresAt = terminatedBrowsers.get(browserId);
    const now = Date.now();
    
    if (now < expiresAt) {
      // Still in cooldown
      const remainingTime = Math.ceil((expiresAt - now) / 1000);
      console.log(`Browser ${browserId} is in cooldown for ${remainingTime} more seconds`);
      return true;
    } else {
      // Cooldown expired, remove from map
      terminatedBrowsers.delete(browserId);
      console.log(`Browser ${browserId} cooldown expired, removed from restrictions`);
      return false;
    }
  }
  
  return false;
}

// Modified: Function to request VM deletion via NATS with request-reply pattern - Issue 1 solution
async function requestVMDeletion(vmId, browserId, reason) {
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
      browserId,
      reason,
      requestedAt: Date.now()
    });
    
    // Use request-reply pattern instead of simple publish - Issue 1 fix
    const reply = await natsConnection.request(
      'vm.delete',
      sc.encode(JSON.stringify({
        requestId: requestId,
        vmId: vmId,
        browserId: browserId,
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
  
  terminatedBrowsers.forEach((expiresAt, browserId) => {
    if (now >= expiresAt) {
      terminatedBrowsers.delete(browserId);
      expiredCount++;
    }
  });
  
  if (expiredCount > 0) {
    console.log(`Cleaned up ${expiredCount} expired browser cooldowns`);
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

// Function to assign a VM to a browser session
async function assignVMToBrowser(browserId, terminalId, ws) {
  // Check if browser already has a VM
  const existingAssignment = browserSessions.get(browserId);
  if (existingAssignment) {
    // Add this terminal to the existing session
    existingAssignment.terminals.add(terminalId);
    
    // Return existing assignment
    return existingAssignment;
  }
  
  // Check if there's a pending request for this browser
  if (pendingVMRequests.has(browserId)) {
    // Add this terminal to the pending request
    const pendingRequest = pendingVMRequests.get(browserId);
    pendingRequest.terminals.add(terminalId);
    pendingRequest.pendingConnections.set(terminalId, ws);
    
    // Let client know we're still waiting for VM
    ws.send(JSON.stringify({
      type: 'vm_creating',
      message: 'Your environment is being created. Please wait...'
    }));
    
    return null;
  }
  
  // Issue 2 solution: Check if browser is in cooldown period
  if (isBrowserInCooldown(browserId)) {
    // Calculate remaining cooldown time
    const remainingSeconds = Math.ceil((terminatedBrowsers.get(browserId) - Date.now()) / 1000);
    
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
    availableVM.browserId = browserId;
    
    // Create browser session
    browserSessions.set(browserId, {
      vmId: availableVM.id,
      vmIp: availableVM.ip,
      assignedAt: Date.now(),
      terminals: new Set([terminalId])
    });
    
    console.log(`Assigned existing VM ${availableVM.id} (${availableVM.ip}) to browser ${browserId}`);
    return browserSessions.get(browserId);
  }
  
  // No existing VM available, request new one via NATS
  if (!natsConnection) {
    console.error('Cannot request VM creation: NATS connection not available');
    return null;
  }
  
  // Create a pending request entry
  pendingVMRequests.set(browserId, {
    requestedAt: Date.now(),
    terminals: new Set([terminalId]),
    pendingConnections: new Map([[terminalId, ws]])
  });
  
  // Notify client that VM is being created
  ws.send(JSON.stringify({
    type: 'vm_creating',
    message: 'Your environment is being created. Please wait...'
  }));
  
  console.log(`No VM available, will request one via NATS for browser ${browserId}`);
  console.log(`NATS connection status:`, natsConnection ? 'Connected' : 'Not connected');  

  // Request VM creation
  try {
    // Generate a unique request ID for this VM creation
    const requestId = uuidv4();
    
    // Send request-reply message
    const reply = await natsConnection.request('vm.create', sc.encode(JSON.stringify({
      requestId: requestId,
      browserId: browserId,
      requestedAt: Date.now()
    })), { timeout: 10000 });
    
    const response = JSON.parse(sc.decode(reply.data));
    console.log(`VM creation request acknowledged: ${JSON.stringify(response)}`);
    
    // The actual VM details will come later on the vm.created subscription
    return null;
  } catch (error) {
    console.error(`Error requesting VM creation for browser ${browserId}:`, error);
    
    // Clean up pending request on error
    pendingVMRequests.delete(browserId);
    
    // Notify client of error
    ws.send(JSON.stringify({
      type: 'error',
      message: 'Failed to create environment. Please try again later.'
    }));
    
    return null;
  }
}

// Function to release a VM when browser session ends
function releaseVM(browserId) {
  const assignment = browserSessions.get(browserId);
  if (!assignment) return;
  
  // Find the VM in the pool
  const vm = vmPool.find(vm => vm.id === assignment.vmId);
  if (vm) {
    // If VM is in our pool, mark it as no longer busy
    vm.busy = false;
    vm.browserId = null;
    console.log(`Released VM ${vm.id} (${vm.ip}) from browser ${browserId}`);
  }
  
  // Remove from sessions map
  browserSessions.delete(browserId);
}

// Create WebSocket server
const wss = new WebSocket.Server({ port: process.env.WS_PORT || 8080 });

wss.on('connection', (ws, req) => {
  // Parse query parameters
  const urlParams = new URL(`http://localhost${req.url}`).searchParams;
  
  // Get or generate browser ID - this should be consistent for tabs in the same browser
  const browserId = urlParams.get('browserid') || `browser_${Date.now()}_${Math.random().toString(36).substring(2, 15)}`;
  
  // Terminal ID is unique per terminal tab
  const terminalId = urlParams.get('terminalid') || `terminal_${Date.now()}_${Math.random().toString(36).substring(2, 15)}`;
  
  console.log(`Terminal ${terminalId} connected from browser ${browserId}`);
  
  // Store session info on the websocket object
  ws.browserId = browserId;
  ws.terminalId = terminalId;
  
  // Handle messages from client
  ws.on('message', async (message) => {
    try {
      const data = JSON.parse(message);
      
      // Handle authentication
      if (data.type === 'auth') {
        try {
          // Optional JWT verification
          //if (process.env.JWT_SECRET && data.token) {
          //  jwt.verify(data.token, process.env.JWT_SECRET);
          //}
          
          // Check if browser is in cooldown before trying to assign a VM
          if (isBrowserInCooldown(browserId)) {
            // Calculate remaining cooldown time
            const remainingSeconds = Math.ceil((terminatedBrowsers.get(browserId) - Date.now()) / 1000);
            
            // Notify client about cooldown
            ws.send(JSON.stringify({
              type: 'error',
              message: `Your previous session recently expired. Please wait ${remainingSeconds} seconds before starting a new session.`
            }));
            
            return;
          }
          
          // Assign a VM for this browser session
          const assignment = await assignVMToBrowser(browserId, terminalId, ws);

          if (!assignment) {
            // This is not an error, just means VM is being created
            // Client has already been notified via ws.send in assignVMToBrowser
            return;
          }
          
          // Connect to the assigned VM
          establishSSHConnection(ws, assignment.vmIp);
          
        } catch (err) {
          ws.send(JSON.stringify({ type: 'error', message: `Authentication failed: ${err.message}` }));
          ws.close();
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
    console.log(`Terminal ${terminalId} from browser ${browserId} disconnected`);
    
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
    
    // Check if there's a pending VM request for this browser
    if (pendingVMRequests.has(browserId)) {
      const pendingRequest = pendingVMRequests.get(browserId);
      pendingRequest.terminals.delete(terminalId);
      pendingRequest.pendingConnections.delete(terminalId);
      
      // If no more terminals are waiting, cancel the VM request
      if (pendingRequest.terminals.size === 0) {
        console.log(`No more terminals waiting for VM, canceling request for browser ${browserId}`);
        pendingVMRequests.delete(browserId);
        
        // Optionally notify VM service to cancel creation if possible
        if (natsConnection) {
          try {
            // Use request-reply pattern for VM cancel as well
            const requestId = uuidv4();
            natsConnection.request(
              'vm.cancel', 
              sc.encode(JSON.stringify({
                requestId: requestId,
                browserId: browserId,
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
    const assignment = browserSessions.get(browserId);
    if (assignment) {
      assignment.terminals.delete(terminalId);
      
      // Only release the VM if no terminals from this browser are connected
      if (assignment.terminals.size === 0) {
        console.log(`No more terminals from browser ${browserId}, releasing VM ${assignment.vmId}`);
        releaseVM(browserId);
      } else {
        console.log(`Browser ${browserId} still has ${assignment.terminals.size} active terminals`);
      }
    }
  });
});

// Periodically check for abandoned pending requests and deletion requests
setInterval(() => {
  const now = Date.now();
  
  // Check for abandoned pending VM requests
  pendingVMRequests.forEach((request, browserId) => {
    // If a pending request is older than 10 minutes, consider it abandoned
    if (now - request.requestedAt > 600000) { // 10 minutes
      console.log(`Pending VM request for browser ${browserId} timed out`);
      
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
      pendingVMRequests.delete(browserId);
      
      // Try to cancel VM creation using request-reply pattern
      if (natsConnection) {
        try {
          const requestId = uuidv4();
          natsConnection.request('vm.cancel', sc.encode(JSON.stringify({
            requestId: requestId,
            browserId: browserId,
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
            browserId: deletion.browserId,
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
  console.log(`Active browser sessions: ${browserSessions.size}`);
  console.log(`Pending VM requests: ${pendingVMRequests.size}`);
  console.log(`Pending VM deletions: ${pendingVMDeletions.size}`);
  console.log(`Browsers in cooldown: ${terminatedBrowsers.size}`);
  
  // Detailed VM Pool Stats
  if (vmPool.length > 0) {
    console.log(`\nVM Pool Details:`);
    vmPool.forEach(vm => {
      console.log(`- VM ${vm.id} (${vm.ip}): ${vm.busy ? 'Busy' : 'Available'}${vm.browserId ? ` (used by ${vm.browserId})` : ''}`);
    });
  }
  
  // Browser sessions with their VMs
  if (browserSessions.size > 0) {
    console.log(`\nBrowser Sessions:`);
    browserSessions.forEach((session, browserId) => {
      const sessionDuration = Math.floor((now - session.assignedAt) / 1000);
      const timeLeft = Math.floor((ENV_SESSION_DURATION - (now - session.assignedAt)) / 1000);
      
      console.log(`- Browser ${browserId}: VM ${session.vmId} (${session.vmIp})`);
      console.log(`  Terminals: ${session.terminals.size}, Duration: ${sessionDuration}s, Time left: ${timeLeft}s`);
    });
  }
  
  // Browsers in cooldown
  if (terminatedBrowsers.size > 0) {
    console.log(`\nBrowsers in Cooldown:`);
    terminatedBrowsers.forEach((expiresAt, browserId) => {
      const cooldownLeft = Math.floor((expiresAt - now) / 1000);
      console.log(`- Browser ${browserId}: ${cooldownLeft}s remaining`);
    });
  }
  
  console.log(`---------------------------------------------\n`);
}, 300000); // Log status every 5 minutes

const port = process.env.WS_PORT || 8080;
console.log(`WebSocket server running on port ${port}`);

// Log initial configuration
console.log(`Server configuration:
- Environment session duration: ${ENV_SESSION_DURATION}ms (${ENV_SESSION_DURATION/60000} minutes)
- Browser cooldown duration: ${BROWSER_COOLDOWN_DURATION}ms (${BROWSER_COOLDOWN_DURATION/60000} minutes)
- Environment check interval: ${ENV_CHECK_INTERVAL}ms
- NATS server: ${NATS_URL}
`);

// Connect to NATS and start the server
connectToNATS().catch(error => {
  console.error('Failed to initialize NATS connection:', error);
});