// API Base URL
const API_BASE = '/api';

// DOM Elements
const dropZone = document.getElementById('drop-zone');
const fileInput = document.getElementById('file-input');
const fileInfo = document.getElementById('file-info');
const fileName = document.getElementById('file-name');
const fileSize = document.getElementById('file-size');
const removeFileBtn = document.getElementById('remove-file');
const uploadBtn = document.getElementById('upload-btn');
const progressContainer = document.getElementById('progress-container');
const progressStatus = document.getElementById('progress-status');
const progressPercentage = document.getElementById('progress-percentage');
const progressBarFill = document.getElementById('progress-bar-fill');
const progressDetails = document.getElementById('progress-details');
const statusMessage = document.getElementById('status-message');

let selectedFile = null;
let currentTaskId = null;
let progressInterval = null;
let isSelectingFile = false;

// Store EventSource globally for cleanup
window.eventSource = null;

// File input change handler
fileInput.addEventListener('change', (e) => {
    if (isSelectingFile) return; // Prevent multiple simultaneous selections
    
    if (e.target.files && e.target.files[0]) {
        isSelectingFile = true;
        handleFileSelect(e.target.files[0]);
        // Reset the input to allow selecting the same file again if needed
        setTimeout(() => {
            e.target.value = '';
            isSelectingFile = false;
        }, 100);
    }
});

// Prevent label click from bubbling up and causing double triggers
const fileInputLabel = document.querySelector('.file-input-label');
if (fileInputLabel) {
    fileInputLabel.addEventListener('click', (e) => {
        e.stopPropagation();
        // Don't prevent default - let the label trigger the input naturally
    });
}

// Drag and drop handlers
// Don't add click handler to dropZone since the label already handles it
// The label inside dropZone will trigger the file input

dropZone.addEventListener('dragover', (e) => {
    e.preventDefault();
    dropZone.classList.add('drag-over');
});

dropZone.addEventListener('dragleave', () => {
    dropZone.classList.remove('drag-over');
});

dropZone.addEventListener('drop', (e) => {
    e.preventDefault();
    dropZone.classList.remove('drag-over');
    const file = e.dataTransfer.files[0];
    if (file) {
        handleFileSelect(file);
    }
});

// Remove file handler
removeFileBtn.addEventListener('click', () => {
    clearFileSelection();
});

// Upload button handler
uploadBtn.addEventListener('click', async () => {
    if (!selectedFile) return;
    await uploadFile();
});

// Handle file selection
function handleFileSelect(file) {
    // Validate file type
    if (!file.name.endsWith('.csv')) {
        showStatus('Please select a CSV file', 'error');
        return;
    }

    // Validate file size (max 100MB)
    const maxSize = 100 * 1024 * 1024; // 100MB
    if (file.size > maxSize) {
        showStatus('File size must be less than 100MB', 'error');
        return;
    }

    selectedFile = file;
    fileName.textContent = file.name;
    fileSize.textContent = formatFileSize(file.size);
    fileInfo.classList.remove('hidden');
    uploadBtn.disabled = false;
    hideStatus();
}

// Clear file selection
function clearFileSelection() {
    selectedFile = null;
    fileInput.value = '';
    fileInfo.classList.add('hidden');
    uploadBtn.disabled = true;
    hideProgress();
    hideStatus();
}

// Format file size
function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
}

// Upload file
async function uploadFile() {
    if (!selectedFile) return;

    const formData = new FormData();
    formData.append('file', selectedFile);

    try {
        uploadBtn.disabled = true;
        uploadBtn.textContent = 'Uploading...';
        showStatus('Uploading file...', 'info');
        hideProgress();

        const response = await fetch(`${API_BASE}/upload`, {
            method: 'POST',
            body: formData
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || 'Upload failed');
        }

        const data = await response.json();
        currentTaskId = data.task_id;
        
        showStatus('File uploaded successfully. Processing...', 'info');
        showProgress();
        startProgressPolling(data.task_id);

    } catch (error) {
        showStatus(`Error: ${error.message}`, 'error');
        uploadBtn.disabled = false;
        uploadBtn.textContent = 'Upload CSV';
    }
}

// Start SSE stream for real-time progress updates
function startProgressPolling(taskId) {
    // Clear any existing interval or event source
    if (progressInterval) {
        clearInterval(progressInterval);
        progressInterval = null;
    }
    
    // Close existing EventSource if any
    if (window.eventSource) {
        window.eventSource.close();
    }

    // Use Server-Sent Events for real-time updates
    const eventSource = new EventSource(`${API_BASE}/upload/${taskId}/stream`);
    window.eventSource = eventSource;
    
    console.log('SSE: Connected to progress stream');

    eventSource.onopen = () => {
        console.log('SSE: Stream opened');
    };

    eventSource.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            console.log('SSE: Received update', data);
            
            // Check if stream is done
            if (data.status === 'done') {
                console.log('SSE: Stream completed');
                eventSource.close();
                window.eventSource = null;
                return;
            }
            
            // Update progress display
            updateProgress(data);

            // Handle completion or failure
            if (data.status === 'completed' || data.status === 'failed') {
                eventSource.close();
                window.eventSource = null;

                if (data.status === 'completed') {
                    uploadBtn.disabled = false;
                    uploadBtn.textContent = 'Upload CSV';
                    showStatus(
                        `Successfully processed ${data.processed_rows || 0} products!`,
                        'success'
                    );
                } else {
                    uploadBtn.disabled = false;
                    uploadBtn.textContent = 'Retry Upload';
                    showStatus(
                        `Upload failed: ${data.error || 'Unknown error'}`,
                        'error',
                        true
                    );
                }
            }
        } catch (error) {
            console.error('Error parsing SSE data:', error);
        }
    };

    eventSource.onerror = (error) => {
        console.error('SSE error:', error);
        console.log('SSE: Falling back to polling');
        // Close and fallback to polling if SSE fails
        eventSource.close();
        window.eventSource = null;
        // Fallback to polling
        fallbackToPolling(taskId);
    };
}

// Fallback to polling if SSE fails
function fallbackToPolling(taskId) {
    // Clear any existing interval
    if (progressInterval) {
        clearInterval(progressInterval);
    }

    // Poll immediately
    checkProgress(taskId);

    // Poll every 1 second as fallback
    progressInterval = setInterval(() => {
        checkProgress(taskId);
    }, 1000);
}

// Check upload progress (fallback method)
async function checkProgress(taskId) {
    try {
        const response = await fetch(`${API_BASE}/upload/${taskId}/progress`);
        
        if (!response.ok) {
            throw new Error('Failed to fetch progress');
        }

        const data = await response.json();
        updateProgress(data);

        // Stop polling if completed or failed
        if (data.status === 'completed' || data.status === 'failed') {
            if (progressInterval) {
                clearInterval(progressInterval);
                progressInterval = null;
            }

            if (data.status === 'completed') {
                uploadBtn.disabled = false;
                uploadBtn.textContent = 'Upload CSV';
                showStatus(
                    `Successfully processed ${data.processed_rows || 0} products!`,
                    'success'
                );
            } else {
                uploadBtn.disabled = false;
                uploadBtn.textContent = 'Retry Upload';
                showStatus(
                    `Upload failed: ${data.error || 'Unknown error'}`,
                    'error',
                    true
                );
            }
        }

    } catch (error) {
        console.error('Error checking progress:', error);
    }
}

// Update progress display
function updateProgress(data) {
    const progress = data.progress || 0;
    const status = data.status || 'processing';
    const message = data.message || 'Processing...';
    const processedRows = data.processed_rows || 0;
    const totalRows = data.total_rows || 0;

    progressBarFill.style.width = `${progress}%`;
    progressPercentage.textContent = `${Math.round(progress)}%`;

    // Update status text
    if (status === 'processing') {
        progressStatus.textContent = message;
    } else if (status === 'completed') {
        progressStatus.textContent = 'Completed';
    } else if (status === 'failed') {
        progressStatus.textContent = 'Failed';
    } else {
        progressStatus.textContent = 'Pending';
    }

    // Update details
    if (totalRows > 0) {
        progressDetails.textContent = `Processed ${processedRows} of ${totalRows} rows`;
    } else {
        progressDetails.textContent = message;
    }
}

// Show progress container
function showProgress() {
    progressContainer.classList.remove('hidden');
}

// Hide progress container
function hideProgress() {
    progressContainer.classList.add('hidden');
    progressBarFill.style.width = '0%';
    progressPercentage.textContent = '0%';
    progressStatus.textContent = 'Processing...';
    progressDetails.textContent = '';
}

// Show status message
function showStatus(message, type = 'info', showRetry = false) {
    statusMessage.textContent = message;
    statusMessage.className = `status-message ${type}`;
    statusMessage.classList.remove('hidden');

    // Add retry button if needed
    if (showRetry) {
        const existingRetry = statusMessage.querySelector('.retry-btn');
        if (!existingRetry) {
            const retryBtn = document.createElement('button');
            retryBtn.className = 'retry-btn';
            retryBtn.textContent = 'Retry';
            retryBtn.addEventListener('click', () => {
                if (selectedFile) {
                    uploadFile();
                }
            });
            statusMessage.appendChild(retryBtn);
        }
    } else {
        const existingRetry = statusMessage.querySelector('.retry-btn');
        if (existingRetry) {
            existingRetry.remove();
        }
    }
}

// Hide status message
function hideStatus() {
    statusMessage.classList.add('hidden');
}

// Navigation handlers
document.querySelectorAll('.nav-link').forEach(link => {
    link.addEventListener('click', (e) => {
        e.preventDefault();
        const target = link.getAttribute('href').substring(1);
        
        // Update active nav link
        document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
        link.classList.add('active');
        
        // Show/hide sections
        document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
        const section = document.getElementById(`${target}-section`);
        if (section) {
            section.classList.add('active');
        }
    });
});

