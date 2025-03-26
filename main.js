// ws-server.js - WebSocket server with browser-based VM allocation using NATS
const WebSocket = require('ws');
const { Client } = require('ssh2');
const fs = require('fs');
const path = require('path');
const jwt = require('jsonwebtoken');
const { connect, StringCodec } = require('nats');
require('dotenv').config();

// Add a new session timeout mechanism with environment removal notification
const ENV_SESSION_DURATION = parseInt(process.env.ENV_SESSION_DURATION || '14400000'); // 4 hours by default
const ENV_CHECK_INTERVAL = 30000; // Check every 30 seconds

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
    
    // Process VM creation messages in a separate function
    processVMCreationMessages(subscription);
    
    // Process VM creation error messages in a separate function
    processVMCreationErrorMessages(errorSubscription);
    
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

// Function to establish SSH connection
function establishSSHConnection(ws, vmIp) {
  const ssh = new Client();
  let stream = null;
  
  // Store SSH connection on websocket
  ws.ssh = ssh;
  
  // Connect to the VM
  console.log(`Connecting terminal ${ws.terminalId} to VM at ${vmIp}`);
  
  // Log SSH connection details (without sensitive info)
  console.log(`SSH connection details: host=${vmIp}, port=${parseInt(process.env.SSH_PORT || '22')}, username=${process.env.VM_USER}`);
  
  // Add timeout for connection
  const sshTimeout = setTimeout(() => {
    console.error(`SSH connection timed out for ${ws.terminalId} to ${vmIp}`);
    ssh.end();
    ws.send(JSON.stringify({ 
      type: 'error', 
      message: 'SSH connection timed out' 
    }));
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
    console.error(`SSH connection error for ${vmIp} (terminal ${ws.terminalId}): ${err.message}`);
    console.error(`Error details:`, err);
    clearTimeout(sshTimeout);
    
    // Check for common SSH errors
    let errorMessage = `SSH error: ${err.message}`;
    if (err.level) {
      errorMessage += ` (level: ${err.level})`;
    }
    
    // Send a more detailed error message to the client
    ws.send(JSON.stringify({ 
      type: 'error', 
      message: errorMessage,
      details: {
        code: err.code,
        level: err.level
      }
    }));
    
    ws.close();
  });
  
  ssh.on('keyboard-interactive', (name, instructions, lang, prompts, finish) => {
    console.log(`SSH keyboard-interactive auth for ${vmIp}`);
    // Generally this won't be triggered with key auth, but logging just in case
  });
  
  ssh.on('handshake', (negotiated) => {
    console.log(`SSH handshake completed for ${vmIp} with algorithms:`, negotiated);
  });
  
  try {
    ssh.connect({
      host: vmIp,
      port: parseInt(process.env.SSH_PORT || '22'),
      username: process.env.VM_USER,
      privateKey: fs.readFileSync(process.env.SSH_PRIVATE_KEY_PATH),
      debug: true, // Enable debug output from the SSH library
      readyTimeout: 30000, // 30 second timeout
      authHandler: ['publickey'] // Force publickey authentication only
    });
  } catch (err) {
    // Handle any synchronous exceptions during connection setup
    console.error(`Exception connecting to SSH for ${vmIp}: ${err.message}`);
    console.error(err.stack);
    ws.send(JSON.stringify({ 
      type: 'error', 
      message: `SSH connection failed: ${err.message}`
    }));
    ws.close();
  }
}

// Function to trigger environment timeouts
function checkEnvironmentTimeout() {
  const now = Date.now();
  
  browserSessions.forEach((assignment, browserId) => {
    // Check if the environment has exceeded its max duration
    const sessionDuration = now - assignment.assignedAt;
    
    if (sessionDuration > ENV_SESSION_DURATION) {
      // Find the VM in the pool
      const vmToDelete = vmPool.find(vm => vm.id === assignment.vmId);
      
      if (vmToDelete) {
        // Find the associated websockets for this browser session
        wss.clients.forEach((ws) => {
          if (ws.browserId === browserId) {
            // Send environment termination message
            try {
              ws.send(JSON.stringify({
                type: 'environment_terminated',
                message: 'Environment session expired. VM scheduled for deletion.',
                deletionReason: 'session_timeout',
                vmId: vmToDelete.id
              }));
              
              // Close the websocket after a short delay to ensure message delivery
              setTimeout(() => {
                ws.close(1000, 'Environment session expired');
              }, 500);
            } catch (error) {
              console.error('Error sending termination message:', error);
            }
          }
        });
        
        // Send message to NATS for VM deletion
        requestVMDeletion(vmToDelete.id, browserId, 'session_timeout');
      }
      
      // Release the VM from browser sessions
      releaseVM(browserId);
    }
  });
}

// Function to request VM deletion via NATS
async function requestVMDeletion(vmId, browserId, reason) {
  if (!natsConnection) {
    console.error('Cannot request VM deletion: NATS connection not available');
    return;
  }
  
  try {
    await natsConnection.publish('vm.delete', sc.encode(JSON.stringify({
      vmId: vmId,
      browserId: browserId,
      reason: reason,
      timestamp: Date.now()
    })));
    
    console.log(`Requested deletion of VM ${vmId} for browser ${browserId} (reason: ${reason})`);
    
    // Remove VM from local pool
    const vmIndex = vmPool.findIndex(vm => vm.id === vmId);
    if (vmIndex !== -1) {
      vmPool.splice(vmIndex, 1);
    }
  } catch (error) {
    console.error(`Error requesting VM deletion for ${vmId}:`, error);
  }
}

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
    // Send request-reply message
    const reply = await natsConnection.request('vm.create', sc.encode(JSON.stringify({
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
            natsConnection.publish('vm.cancel', sc.encode(JSON.stringify({
              browserId: browserId,
              timestamp: Date.now()
            })));
          } catch (e) {
            console.error('Error publishing cancel message:', e);
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
  
  // Periodically check for idle browser sessions
  const SESSION_TIMEOUT = parseInt(process.env.SESSION_TIMEOUT || '1800000'); // Default 30 minutes
  setInterval(() => {
    const now = Date.now();
    
    // Check active sessions
    browserSessions.forEach((assignment, browserId) => {
      if (now - assignment.assignedAt > SESSION_TIMEOUT) {
        // Check if there are any active terminals
        if (assignment.terminals.size === 0) {
          console.log(`Browser session ${browserId} timed out, releasing VM ${assignment.vmId}`);
          
          // Request VM deletion via NATS
          requestVMDeletion(assignment.vmId, browserId, 'session_timeout');
          releaseVM(browserId);
        } else {
          // Update the timestamp if there are active terminals
          assignment.assignedAt = now;
        }
      }
    });
    
    // Also check pending requests that might have been abandoned
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
        
        // Try to cancel VM creation
        if (natsConnection) {
          try {
            natsConnection.publish('vm.cancel', sc.encode(JSON.stringify({
              browserId: browserId,
              reason: 'timeout',
              timestamp: Date.now()
            })));
          } catch (e) {
            console.error('Error publishing cancel message:', e);
          }
        }
      }
    });
  }, 60000); // Check every minute
  
  const port = process.env.WS_PORT || 8080;
  console.log(`WebSocket server running on port ${port}`);
  
  // Connect to NATS and start the server
  connectToNATS().catch(error => {
    console.error('Failed to initialize NATS connection:', error);
  });