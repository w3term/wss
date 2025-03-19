// ws-server.js - WebSocket server with browser-based VM allocation
const WebSocket = require('ws');
const { Client } = require('ssh2');
const fs = require('fs');
const path = require('path');
const jwt = require('jsonwebtoken');
require('dotenv').config();

// VM Pool configuration - read path from environment variables
const VM_POOL_FILE = process.env.VM_POOL_FILE || path.join(__dirname, 'vm-pool.json');

// Add a new session timeout mechanism with environment removal notification
const ENV_SESSION_DURATION = parseInt(process.env.ENV_SESSION_DURATION || '14400000'); // 4 hours by default
const ENV_CHECK_INTERVAL = 30000; // Check every 30 seconds

// In-memory VM pool
let vmPool = [];
let vmPoolLastModified = 0;

// Map to track browser sessions and their assigned VMs
const browserSessions = new Map();

// Fonction to trigger environment timeouts
function checkEnvironmentTimeout() {
  const now = Date.now();
  
  browserSessions.forEach((assignment, browserId) => {
    // Check if the environment has exceeded its max duration
    const sessionDuration = now - assignment.assignedAt;
    
    if (sessionDuration > ENV_SESSION_DURATION) {
      // Find the VM in the pool
      const vmToDelete = vmPool.find(vm => vm.id === assignment.vmId);
      
      if (vmToDelete) {
        // Mark the VM as ready for deletion
        vmToDelete.busy = false;
        vmToDelete.deletion_needed = true;
        
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
        
        // Update the VM pool file with deletion_needed status
        try {
          // First read the file to get the latest structure
          const data = fs.readFileSync(VM_POOL_FILE, 'utf8');
          let filePool = JSON.parse(data);
          
          // Update the specific VM's status
          const fileVMIndex = filePool.findIndex(vm => vm.id === vmToDelete.id);
          if (fileVMIndex !== -1) {
            filePool[fileVMIndex] = {
              ...filePool[fileVMIndex],
              busy: false,
              deletion_needed: true
            };
            
            // Write updated data back to file
            fs.writeFileSync(VM_POOL_FILE, JSON.stringify(filePool, null, 2), 'utf8');
            console.log(`VM ${vmToDelete.id} marked for deletion`);
          }
        } catch (error) {
          console.error('Error updating VM pool file for deletion:', error);
        }
      }
      
      // Release the VM from browser sessions
      releaseVM(browserId);
    }
  });
}

// Start the environment timeout check interval
const environmentTimeoutInterval = setInterval(checkEnvironmentTimeout, ENV_CHECK_INTERVAL);

// Optional: Cleanup interval on server shutdown
//process.on('SIGINT', () => {
//  clearInterval(environmentTimeoutInterval);
//  console.log('Environment timeout checker stopped');
//});

// Function to update VM status in the pool file
function updateVMPoolFile() {
  try {
    // First read the file to get the latest structure
    const data = fs.readFileSync(VM_POOL_FILE, 'utf8');
    let filePool = JSON.parse(data);
    
    // Update busy status in the file based on memory status
    filePool = filePool.map(fileVM => {
      const memoryVM = vmPool.find(vm => vm.id === fileVM.id);
      if (memoryVM) {
        return {
          ...fileVM,
          busy: memoryVM.busy,
          browserId: memoryVM.browserId || null
        };
      }
      return fileVM;
    });
    
    // Write updated data back to file
    fs.writeFileSync(VM_POOL_FILE, JSON.stringify(filePool, null, 2), 'utf8');
    console.log('VM pool file updated with current busy status');
    
  } catch (error) {
    console.error('Error updating VM pool file:', error);
  }
}

// Function to read VM pool configuration
function readVMPoolConfig() {
  try {
    const stats = fs.statSync(VM_POOL_FILE);
    
    // Check if file has been modified since last read
    if (stats.mtimeMs > vmPoolLastModified) {
      console.log('VM pool configuration file modified, reloading...');
      
      const data = fs.readFileSync(VM_POOL_FILE, 'utf8');
      const newPool = JSON.parse(data);
      
      // Update only the VMs list, keeping busy status from memory
      newPool.forEach(newVM => {
        const existingVM = vmPool.find(vm => vm.id === newVM.id);
        if (existingVM) {
          // Keep existing busy status
          newVM.busy = existingVM.busy;
          newVM.browserId = existingVM.browserId;
        }
      });
      
      vmPool = newPool;
      vmPoolLastModified = stats.mtimeMs;
      console.log(`VM pool updated with ${vmPool.length} VMs`);
    }
  } catch (error) {
    console.error('Error reading VM pool configuration:', error);
    // If file doesn't exist yet, create empty pool with fallback VM
    if (error.code === 'ENOENT') {
      console.log('VM pool file not found, using fallback VM configuration');
      vmPool = [{
        id: "fallback",
        ip: process.env.VM_HOST || "localhost", // Use VM_HOST as fallback
        busy: false
      }];
    }
  }
}

// Initialize VM pool
readVMPoolConfig();

// Watch for changes to VM pool file
try {
  fs.watch(path.dirname(VM_POOL_FILE), (eventType, filename) => {
    if (filename === path.basename(VM_POOL_FILE) && eventType === 'change') {
      // Add a small delay to ensure file is fully written
      setTimeout(readVMPoolConfig, 100);
    }
  });
  console.log(`Watching for changes to VM pool file: ${VM_POOL_FILE}`);
} catch (error) {
  console.error(`Error watching VM pool file: ${error.message}`);
}

// Function to assign a VM to a browser session
function assignVMToBrowser(browserId) {
  // Check if browser already has a VM
  const existingAssignment = browserSessions.get(browserId);
  if (existingAssignment) {
    return existingAssignment;
  }
  
  // Find available VM that does NOT have deletion_needed flag
  const availableVM = vmPool.find(vm => 
    !vm.busy && 
    (!vm.deletion_needed || vm.deletion_needed === false)
  );

  if (!availableVM) {
    console.log('No VMs available');
    return null; // No VMs available
  }
  
  // Mark VM as busy
  availableVM.busy = true;
  availableVM.browserId = browserId;
  
  // Add to browser sessions map
  browserSessions.set(browserId, {
    vmId: availableVM.id,
    vmIp: availableVM.ip,
    assignedAt: Date.now(),
    terminals: new Set() // Track terminals using this VM
  });
  
  console.log(`Assigned VM ${availableVM.id} (${availableVM.ip}) to browser ${browserId}`);

  // Update the VM pool file with new status
  updateVMPoolFile();

  return browserSessions.get(browserId);
}

// Function to release a VM when browser session ends
function releaseVM(browserId) {
  const assignment = browserSessions.get(browserId);
  if (!assignment) return;
  
  // Find the VM in the pool
  const vm = vmPool.find(vm => vm.id === assignment.vmId);
  if (vm) {
    vm.busy = false;
    vm.browserId = null;
    console.log(`Released VM ${vm.id} (${vm.ip}) from browser ${browserId}`);
  }
  
  // Remove from sessions map
  browserSessions.delete(browserId);

  // Update the VM pool file with new status
  updateVMPoolFile();
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
  
  // Setup SSH connection
  const ssh = new Client();
  let stream = null;
  
  // Handle messages from client
  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      
      // Handle authentication
      if (data.type === 'auth') {
        try {
          // Optional JWT verification
          if (process.env.JWT_SECRET && data.token) {
            jwt.verify(data.token, process.env.JWT_SECRET);
          }
          
          // Assign a VM for this browser session
          const assignment = assignVMToBrowser(browserId);
          
          if (!assignment) {
            ws.send(JSON.stringify({ 
              type: 'error', 
              message: 'No VMs available. Please try again later.' 
            }));
            return;
          }
          
          // Add this terminal to the browser session
          assignment.terminals.add(terminalId);
          
          // Connect to the assigned VM
          console.log(`Connecting terminal ${terminalId} to VM at ${assignment.vmIp}`);
          ssh.connect({
            host: assignment.vmIp,
            port: parseInt(process.env.SSH_PORT || '22'),
            username: process.env.VM_USER,
            privateKey: fs.readFileSync(process.env.SSH_PRIVATE_KEY_PATH)
          });
          
        } catch (err) {
          ws.send(JSON.stringify({ type: 'error', message: `Authentication failed: ${err.message}` }));
          ws.close();
        }
      }
      
      // Handle raw terminal data
      else if (data.type === 'data' && stream) {
        stream.write(data.data);
      }
      
      // Handle terminal resize events
      else if (data.type === 'resize' && stream) {
        stream.setWindow(data.rows, data.cols, 0, 0);
      }
    } catch (err) {
      console.error('Error processing message:', err);
      ws.send(JSON.stringify({ type: 'error', message: `Message processing error: ${err.message}` }));
    }
  });
  
  // Handle SSH events
  ssh.on('ready', () => {
    ws.send(JSON.stringify({ type: 'connected', message: 'SSH connection established' }));
    
    // Request a PTY (pseudo-terminal) with specific size
    ssh.shell({ 
      term: 'xterm-256color',
      rows: 24,
      cols: 80
    }, (err, shellStream) => {
      if (err) {
        ws.send(JSON.stringify({ type: 'error', message: 'Failed to open shell' }));
        return;
      }
      
      stream = shellStream;
      
      // Send raw SSH output directly to client
      stream.on('data', (data) => {
        ws.send(JSON.stringify({ 
          type: 'data', 
          data: data.toString('utf-8')
        }));
      });
      
      stream.stderr.on('data', (data) => {
        ws.send(JSON.stringify({ 
          type: 'data', 
          data: data.toString('utf-8')
        }));
      });
      
      stream.on('close', () => {
        ws.send(JSON.stringify({ type: 'closed', message: 'SSH connection closed' }));
        ssh.end();
      });
    });
  });
  
  ssh.on('error', (err) => {
    ws.send(JSON.stringify({ type: 'error', message: `SSH error: ${err.message}` }));
    ws.close();
  });
  
  // Handle client disconnect
  ws.on('close', () => {
    console.log(`Terminal ${terminalId} from browser ${browserId} disconnected`);
    if (stream) stream.close();
    ssh.end();
    
    // Remove this terminal from the browser session
    const assignment = browserSessions.get(browserId);
    if (assignment) {
      assignment.terminals.delete(terminalId);
      
      // Only release the VM if no terminals from this browser are connected
      if (assignment.terminals.size === 0) {
        console.log(`No more terminals from browser ${browserId}, releasing VM`);
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
  
  browserSessions.forEach((assignment, browserId) => {
    if (now - assignment.assignedAt > SESSION_TIMEOUT) {
      // Check if there are any active terminals
      if (assignment.terminals.size === 0) {
        console.log(`Browser session ${browserId} timed out, releasing VM ${assignment.vmId}`);
        releaseVM(browserId);
      } else {
        // Update the timestamp if there are active terminals
        assignment.assignedAt = now;
      }
    }
  });
}, 60000); // Check every minute

const port = process.env.WS_PORT || 8080;
console.log(`WebSocket server running on port ${port}`);