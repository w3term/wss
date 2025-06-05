const WebSocket = require('ws');
const { Client } = require('ssh2');
const fs = require('fs');
const { connect, StringCodec } = require('nats');
const { v4: uuidv4 } = require('uuid');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
require('dotenv').config();

// NATS connection configuration
const NATS_URL = process.env.NATS_URL || 'nats://nats:4222';
let natsConnection = null;
const sc = StringCodec();

// Connection tracking
const activeTerminals = new Map(); // Map of terminalId -> { ws, sshClient, stream }
const userTerminals = new Map();   // Map of userId -> Set of terminalIds

// Maximum number of terminals per user
const MAX_TERMINALS_PER_USER = 4;

// Connect to NATS
async function connectToNATS() {
  try {
    natsConnection = await connect({ servers: NATS_URL });
    console.log(`Connected to NATS server at ${NATS_URL}`);
    
    // Subscribe to VM-related notifications
    subscribeToNotifications();
    
  } catch (error) {
    console.error('Error connecting to NATS:', error);
    setTimeout(connectToNATS, 5000); // Retry connection after delay
  }
}

// Subscribe to necessary NATS subjects for notifications
async function subscribeToNotifications() {
  // VM creation completed notification
  const vmCreatedSub = natsConnection.subscribe('vm.created');
  processVMCreatedNotifications(vmCreatedSub);
  
  // VM session expired notification
  const vmExpiringSub = natsConnection.subscribe('vm.expiring');
  processVMExpiringNotifications(vmExpiringSub);
}

// Process VM created notifications
async function processVMCreatedNotifications(subscription) {
  for await (const msg of subscription) {
    try {
      const vmData = JSON.parse(sc.decode(msg.data));
      console.log(`Received VM creation notification for user: ${vmData.userId}`);
      
      // Find all terminals for this user and establish SSH connections
      if (userTerminals.has(vmData.userId)) {
        const terminals = userTerminals.get(vmData.userId);
        
        for (const terminalId of terminals) {
          const terminalData = activeTerminals.get(terminalId);
          if (terminalData && terminalData.ws.readyState === WebSocket.OPEN) {
            // Notify client that VM is ready
            terminalData.ws.send(JSON.stringify({
              type: 'vm_ready',
              message: 'Your environment is ready',
              vmId: vmData.vmId,
              ip: vmData.ip
            }));
            
            // Establish SSH connection
            establishSSHConnection(terminalData.ws, vmData.ip);
          }
        }
      }
    } catch (error) {
      console.error('Error processing vm.created notification:', error);
    }
  }
}

// Process VM expiring notifications
async function processVMExpiringNotifications(subscription) {
  for await (const msg of subscription) {
    try {
      // Parse the message data
      const expData = JSON.parse(sc.decode(msg.data));
      console.log(`Received VM expiring notification for user: ${expData.userId}, VM: ${expData.vmId}`);
      
      // Always prepare a success response
      const expiryReplyData = { acknowledged: true };
      let terminalsFound = false;
      
      // Only attempt notification if the user has terminals
      if (userTerminals.has(expData.userId)) {
        const terminals = userTerminals.get(expData.userId);
        terminalsFound = terminals.size > 0;
        
        // Notify all terminals for this user
        for (const terminalId of terminals) {
          const terminalData = activeTerminals.get(terminalId);
          if (terminalData && terminalData.ws.readyState === WebSocket.OPEN) {
            console.log(`Notifying terminal ${terminalId} about VM expiration`);

            // Set cooldown flag
            terminalData.ws.inCooldown = true;

            // Send session expiration message to client
            terminalData.ws.send(JSON.stringify({
              type: 'environment_terminated',
              message: 'Your session has expired.',
              reason: expData.reason,
              cooldown: expData.cooldown
            }));
            
            // Close the connection immediately after sending
            setTimeout(() => closeTerminalConnection(terminalId), 500);
          }
        }
      }
      
      console.log(`Responding to VM expiring for user ${expData.userId} (terminals found: ${terminalsFound})`);
      
      // Always respond to the request to prevent timeout
      if (msg.reply) {
        msg.respond(sc.encode(JSON.stringify(expiryReplyData)));
      } else {
        console.log(`Warning: No reply subject in vm.expiring message`);
      }
    } catch (error) {
      console.error('Error processing vm.expiring notification:', error);
      
      // Respond with failure even on error to prevent timeout
      if (msg.reply) {
        msg.respond(sc.encode(JSON.stringify({ 
          acknowledged: false, 
          error: error.message 
        })));
      } else {
        console.error('No reply subject available to respond with error');
      }
    }
  }
}

// Establish SSH connection to a VM
function establishSSHConnection(ws, vmIp) {
  // Configuration
  const MAX_RETRIES = 3;
  const INITIAL_RETRY_DELAY = 3000; // 3 seconds
  const MAX_RETRY_DELAY = 15000; // 15 seconds
  
  let currentRetry = 0;
  let retryDelay = INITIAL_RETRY_DELAY;

  // Skip SSH connection if user is in cooldown
  if (ws.inCooldown) {
    console.log(`Skipping SSH connection for ${ws.terminalId} - user ${ws.userId} is in cooldown`);
    return;
  }
  
  // Check if user is in cooldown by checking the user's terminals set
  if (userTerminals.has(ws.userId)) {
    const cooldownKey = `cooldown_${ws.userId}`;
    if (global[cooldownKey]) {
      console.log(`User ${ws.userId} is in cooldown - blocking SSH connection`);
      ws.inCooldown = true;
      return;
    }
  }

  // Skip connection if no valid IP is provided
  if (!vmIp || vmIp === 'undefined' || vmIp === 'null') {
    console.log(`Skipping SSH connection - no valid IP address provided for terminal ${ws.terminalId}`);
    return;
  }

  // Function to attempt SSH connection with retries
  function attemptSSHConnection() {
    const ssh = new Client();
    let stream = null;
    
    // Store SSH connection on websocket
    ws.ssh = ssh;
    
    // Connect to the VM
    console.log(`SSH connection attempt ${currentRetry + 1}/${MAX_RETRIES} for terminal ${ws.terminalId} to VM at ${vmIp}`);
    
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
        cols: 120,
        modes: {
          ECHO: 1,
          ICANON: 1,
          ISIG: 1
        }
      }, (err, shellStream) => {
        if (err) {
          console.error(`Failed to open shell on ${vmIp}: ${err.message}`);
          ws.send(JSON.stringify({ type: 'error', message: `Failed to open shell: ${err.message}` }));
          return;
        }
        
        stream = shellStream;
        ws.stream = stream;
        
        // Send SSH output to client
        stream.on('data', (data) => {
          const dataStr = data.toString('utf-8');
          ws.send(JSON.stringify({ 
            type: 'data', 
            data: dataStr
          }));
        });
        
        stream.stderr.on('data', (data) => {
          const errorText = data.toString('utf-8');
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
    
    try {
      ssh.connect({
        host: vmIp,
        port: parseInt(process.env.SSH_PORT || '22'),
        username: process.env.VM_USER,
        privateKey: fs.readFileSync(process.env.SSH_PRIVATE_KEY_PATH || '/etc/keys/cka'),
        readyTimeout: 30000,
        keepaliveInterval: 5000,
        keepaliveCountMax: 5
      });
    } catch (err) {
      clearTimeout(sshTimeout);
      handleConnectionFailure(err, ssh);
    }
  }
  
  // Function to handle connection failures and retry if appropriate
  function handleConnectionFailure(err, ssh) {
    console.error(`SSH connection error for ${vmIp} (terminal ${ws.terminalId}): ${err.message}`);
    
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
        retryDelay = Math.min(retryDelay * 1.5, MAX_RETRY_DELAY);
      }, retryDelay);
    } else {
      // No more retries, send final error to client
      ws.send(JSON.stringify({ 
        type: 'error', 
        message: `SSH connection failed after ${MAX_RETRIES} attempts: ${err.message}`
      }));
      
      ws.close();
    }
  }
  
  // Start first attempt
  attemptSSHConnection();
}

// Close a terminal connection properly
function closeTerminalConnection(terminalId) {
  const terminalData = activeTerminals.get(terminalId);
  if (!terminalData) return;
  
  // Close SSH stream if it exists
  if (terminalData.stream) {
    try {
      terminalData.stream.close();
    } catch (e) {
      console.error(`Error closing stream for terminal ${terminalId}:`, e);
    }
  }
  
  // Close SSH connection if it exists
  if (terminalData.ssh) {
    try {
      terminalData.ssh.end();
    } catch (e) {
      console.error(`Error ending SSH connection for terminal ${terminalId}:`, e);
    }
  }
  
  // Close WebSocket if it's open
  if (terminalData.ws && terminalData.ws.readyState === WebSocket.OPEN) {
    try {
      terminalData.ws.close();
    } catch (e) {
      console.error(`Error closing WebSocket for terminal ${terminalId}:`, e);
    }
  }
  
  // Remove terminal from tracking
  activeTerminals.delete(terminalId);
  
  // Remove from user's terminals if possible
  if (terminalData.userId && userTerminals.has(terminalData.userId)) {
    const userTerminalSet = userTerminals.get(terminalData.userId);
    userTerminalSet.delete(terminalId);
    
    // If user has no more terminals, remove the user entry
    if (userTerminalSet.size === 0) {
      userTerminals.delete(terminalData.userId);
    }
  }
}

// Create WebSocket server
const wss = new WebSocket.Server({ 
  port: process.env.WS_PORT || 8081,
});

// websocket connection handler
wss.on('connection', async (ws, req) => {
  console.log("New WebSocket connection attempt");
  //console.log("URL:", req.url);

  // Parse query parameters
  const urlParams = new URL(`http://localhost${req.url}`).searchParams;
  //console.log("URL params:", Object.fromEntries(urlParams.entries()));

  // Get token from URL parameters
  const authToken = urlParams.get('token');

  if (!authToken) {
    console.warn('Connection attempt with no auth token');
    ws.send(JSON.stringify({ type: 'error', message: 'Authentication required' }));
    ws.close();
    return;
  }

  // Log the JWT secret configuration
  if (!process.env.JWT_SECRET) {
    console.error("JWT_SECRET is not defined in environment variables!");
    ws.send(JSON.stringify({ type: 'error', message: 'Server authentication configuration error' }));
    ws.close();
    return;
  }

  // Verify JWT token
  let decodedToken;
  try {
    decodedToken = jwt.verify(authToken, process.env.JWT_SECRET);
    
    req.verifiedUser = {
      userId: decodedToken.username,
      githubId: decodedToken.githubId
    };
  } catch (error) {
    if (error.name === 'TokenExpiredError') {
      console.error("Token has expired:", error.message);
      ws.send(JSON.stringify({ type: 'error', message: 'Token expired' }));
      ws.close();
      return;
    } else if (error.name === 'JsonWebTokenError') {
      console.error("Invalid token:", error.message);
      ws.send(JSON.stringify({ type: 'error', message: 'Invalid token' }));
      ws.close();
      return;
    } else {
      console.error("Token verification failed:", error.message);
      ws.send(JSON.stringify({ type: 'error', message: 'Authentication required' }));
      ws.close();
      return;
    }
  }

  // Use the verified user ID from the token
  const userId = req.verifiedUser.userId;

  // Get VM type from URL parameters
  const vmType = urlParams.get('vmtype');
  console.log(`Type of VM requested: ${vmType}`);

  // Terminal ID is unique per terminal tab
  const terminalId = urlParams.get('terminalid') || `terminal_${Date.now()}_${Math.random().toString(36).substring(2, 15)}`;
  
  // Check if we should force existing session check
  const checkSession = urlParams.get('checksession') === 'true';
  
  console.log(`Terminal ${terminalId} connected from user ${userId}${checkSession ? ' (with session check)' : ''}`);
  
  // Store session info on the websocket object
  ws.userId = userId;
  ws.terminalId = terminalId;
  ws.vmType = vmType;
  
  // Generate a connection-specific security token
  // This can be used to validate commands on the WebSocket server
  const connectionToken = crypto.randomBytes(16).toString('hex');
  ws.connectionToken = connectionToken;
  
  // Track this terminal
  activeTerminals.set(terminalId, { ws, userId, ssh: null, stream: null, connectionToken });
  
  // Add to user's terminals set
  if (!userTerminals.has(userId)) {
    userTerminals.set(userId, new Set());
  }

  // Check terminal limit before adding this new terminal
  const userTerminalSet = userTerminals.get(userId);
  if (userTerminalSet.size >= MAX_TERMINALS_PER_USER) {
    console.log(`User ${userId} exceeded terminal limit (${MAX_TERMINALS_PER_USER})`);
    ws.send(JSON.stringify({ 
      type: 'error', 
      message: `You have reached the maximum limit of ${MAX_TERMINALS_PER_USER} terminal sessions.`
    }));
    ws.close();
    return;
  }
  
  // If within limits, proceed with adding the terminal
  userTerminals.get(userId).add(terminalId);
  
  // Send the connection token to the client
  ws.send(JSON.stringify({
    type: 'connection_established',
    connectionToken: connectionToken
  }));
  
  // When checkSession is true, immediately check for existing sessions
  if (checkSession) {
    try {
      // Check if user has an active VM session
      const sessionReply = await natsConnection.request('vm.check', 
        sc.encode(JSON.stringify({
          userId: userId,
          terminalId: terminalId,
          forceCheck: true // Add this flag to ensure VM manager checks thoroughly
        })),
        { timeout: 10000 }
      );
      
      const sessionData = JSON.parse(sc.decode(sessionReply.data));
      console.log("Session check reply from instances manager:", sessionData);

      if (sessionData.status === 'active') {
        ws.send(JSON.stringify({
          type: 'vm_ready',
          message: 'Your environment is ready',
          vmId: sessionData.vmId,
          ip: sessionData.vmIp
        }));
        
        // Connect to the VM
        establishSSHConnection(ws, sessionData.vmIp);
      }
    } catch (error) {
      console.error('Error checking for existing sessions:', error);
    }
  }
  
  // Handle messages from client
  ws.on('message', async (message) => {
    try {
      // Try to parse as JSON
      const data = JSON.parse(message);

      // Process message based on type
      switch (data.type) {
        // Handle authentication/VM session request
        case 'auth':
          // Check if user can have a VM session
          const sessionReply = await natsConnection.request('vm.check', 
            sc.encode(JSON.stringify({
              userId: userId,
              terminalId: terminalId
            })),
            { timeout: 10000 }
          );
          
          const sessionData = JSON.parse(sc.decode(sessionReply.data));
          console.log("Reply from instances manager", sessionData);

          if (sessionData.status === 'cooldown') {
            // Mark the websocket as in cooldown
            ws.inCooldown = true;

            // User is in cooldown period
            ws.send(JSON.stringify({
              type: 'error',
              message: 'Your session has expired',
              cooldown: sessionData.cooldown
            }));

              // Close this connection immediately after sending the message
              setTimeout(() => ws.close(), 500);
            return;
          }

          // Connect to the active VM existing for the current user
          if (sessionData.status === 'active') {
            ws.send(JSON.stringify({
              type: 'vm_ready',
              message: 'Your environment is ready',
              vmId: sessionData.vmId,
              ip: sessionData.vmIp
            }));
            
            // Connect to the VM
            establishSSHConnection(ws, sessionData.vmIp);
            return;
          }
          
          if (sessionData.status === 'creating') {
            // VM is being created, notification will come later
            ws.send(JSON.stringify({
              type: 'vm_creating',
              message: 'Your environment is being created. Please wait...'
            }));
            return;
          }
          
          // Request a new VM
          const vmReply = await natsConnection.request('vm.create', 
            sc.encode(JSON.stringify({
              requestId: uuidv4(),
              userId: userId,
              terminalId: terminalId,
              vmType: vmType,
              requestedAt: Date.now()
            })),
            { timeout: 10000 }
          );
          
          const vmData = JSON.parse(sc.decode(vmReply.data));
          
          if (vmData.status === 'accepted') {
            // Request accepted, wait for VM creation
            ws.send(JSON.stringify({
              type: 'vm_creating',
              message: 'Your environment is being created. Please wait...'
            }));
          } else if (vmData.status === 'error') {
            // Error creating VM
            ws.send(JSON.stringify({
              type: 'error',
              message: `Failed to create environment: ${vmData.error}`
            }));
          }
          break;
          
        // Handle terminal input - high-frequency data, no security check needed
        case 'data':
          if (ws.stream) {
            ws.stream.write(data.data);
          }
          break;
          
        case 'explicit_close':
          console.log(`Explicit close request received for terminal ${data.terminalId || terminalId}`);
          // No response needed, the WebSocket will be closed anyway
          break;

        // Handle terminal resize events
        case 'resize':
          if (ws.stream) {
            ws.stream.setWindow(data.rows, data.cols, 0, 0);
          }
          break;
          
        default:
          console.warn(`Unknown message type: ${data.type}`);
      }
    } catch (err) {
      // Not valid JSON, try to treat as raw terminal data
      if (ws.stream) {
        try {
          ws.stream.write(message);
        } catch (streamErr) {
          console.error('Error writing to stream:', streamErr);
          ws.send(JSON.stringify({ 
            type: 'error', 
            message: `Stream error: ${streamErr.message}` 
          }));
        }
      } else {
        console.error('Error processing message:', err);
        ws.send(JSON.stringify({ 
          type: 'error', 
          message: `Message processing error: ${err.message}` 
        }));
      }
    }
  });
  
  // Handle client disconnect
  ws.on('close', () => {
    console.log(`Terminal ${terminalId} from user ${userId} disconnected`);
    
    // Clean up connections
    closeTerminalConnection(terminalId);

    // Notify instance manager about terminal disconnection
    if (natsConnection) {
      try {
        natsConnection.publish('terminal.disconnected', sc.encode(JSON.stringify({
          userId: userId,
          terminalId: terminalId,
          timestamp: Date.now()
        })));
      } catch (e) {
        console.error('Error publishing terminal disconnect event:', e);
      }
    }
    
    // Clean up connections
    closeTerminalConnection(terminalId);
  });
});

// Log active connections periodically
setInterval(() => {
  console.log(`\n--- WebSocket Server Status ---`);
  console.log(`Active terminals: ${activeTerminals.size}`);
  console.log(`Active users: ${userTerminals.size}`);
  
  // Show active users and their terminal counts
  if (userTerminals.size > 0) {
    console.log(`\nActive user sessions:`);
    for (const [userId, terminals] of userTerminals.entries()) {
      console.log(`- User ${userId}: ${terminals.size} terminal(s)`);
    }
  }
  
  console.log(`------------------------------\n`);
}, 300000); // Log every 5 minutes

// Handle cleanup on shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down WebSocket server...');
  
  // Close all active connections
  for (const terminalId of activeTerminals.keys()) {
    closeTerminalConnection(terminalId);
  }
  
  // Drain NATS connection
  if (natsConnection) {
    await natsConnection.drain();
    console.log('NATS connection drained');
  }
  
  process.exit(0);
});

// Start server
const port = process.env.WS_PORT || 8081;
console.log(`WebSocket server running on port ${port}`);
console.log(`NATS server: ${NATS_URL}`);

// Connect to NATS and start the server
connectToNATS().catch(error => {
  console.error('Failed to initialize NATS connection:', error);
});