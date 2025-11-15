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
            // Load products when products section is shown
            if (target === 'products') {
                loadProducts();
            }
            // Load webhooks when webhooks section is shown
            if (target === 'webhooks') {
                loadWebhooks();
            }
        }
    });
});

// ==================== PRODUCT MANAGEMENT ====================

// Product state
let currentPage = 1;
let pageSize = 20;
let totalPages = 1;
let currentFilters = {};
let editingProductId = null;
let filterTimeout = null;

// DOM Elements for Products
const productsTbody = document.getElementById('products-tbody');
const createProductBtn = document.getElementById('create-product-btn');
const bulkDeleteBtn = document.getElementById('bulk-delete-btn');
const productModal = document.getElementById('product-modal');
const productForm = document.getElementById('product-form');
const modalTitle = document.getElementById('modal-title');
const closeModalBtn = document.getElementById('close-modal-btn');
const cancelProductBtn = document.getElementById('cancel-product-btn');
const confirmModal = document.getElementById('confirm-modal');
const confirmMessage = document.getElementById('confirm-message');
const confirmActionBtn = document.getElementById('confirm-action-btn');
const cancelConfirmBtn = document.getElementById('cancel-confirm-btn');
const closeConfirmBtn = document.getElementById('close-confirm-btn');
const prevPageBtn = document.getElementById('prev-page-btn');
const nextPageBtn = document.getElementById('next-page-btn');
const pageNumbers = document.getElementById('page-numbers');
const paginationInfo = document.getElementById('pagination-info');
const filterSku = document.getElementById('filter-sku');
const filterName = document.getElementById('filter-name');
const filterDescription = document.getElementById('filter-description');
const filterActive = document.getElementById('filter-active');
const clearFiltersBtn = document.getElementById('clear-filters-btn');

// Load products from API
async function loadProducts() {
    try {
        productsTbody.innerHTML = '<tr><td colspan="6" class="loading">Loading products...</td></tr>';
        
        const params = new URLSearchParams({
            page: currentPage,
            page_size: pageSize,
            ...currentFilters
        });
        
        const response = await fetch(`${API_BASE}/products?${params}`);
        if (!response.ok) throw new Error('Failed to fetch products');
        
        const data = await response.json();
        
        // Update pagination state
        totalPages = data.total_pages;
        currentPage = data.page;
        
        // Render products
        renderProducts(data.products);
        updatePagination(data);
        
    } catch (error) {
        console.error('Error loading products:', error);
        productsTbody.innerHTML = `<tr><td colspan="6" class="error">Error loading products: ${error.message}</td></tr>`;
    }
}

// Render products table
function renderProducts(products) {
    if (products.length === 0) {
        productsTbody.innerHTML = '<tr><td colspan="6" class="empty">No products found</td></tr>';
        return;
    }
    
    productsTbody.innerHTML = products.map(product => `
        <tr>
            <td>${product.id}</td>
            <td>${escapeHtml(product.sku)}</td>
            <td>${escapeHtml(product.name)}</td>
            <td>${escapeHtml(product.description || '')}</td>
            <td><span class="badge ${product.active ? 'badge-success' : 'badge-inactive'}">${product.active ? 'Active' : 'Inactive'}</span></td>
            <td class="actions">
                <button class="btn-icon btn-edit" onclick="editProduct(${product.id})" title="Edit">✏️</button>
                <button class="btn-icon btn-delete" onclick="deleteProduct(${product.id})" title="Delete">🗑️</button>
            </td>
        </tr>
    `).join('');
}

// Update pagination controls
function updatePagination(data) {
    // Update info
    const start = data.total === 0 ? 0 : (data.page - 1) * data.page_size + 1;
    const end = Math.min(data.page * data.page_size, data.total);
    paginationInfo.textContent = `Showing ${start} - ${end} of ${data.total} products`;
    
    // Update buttons
    prevPageBtn.disabled = data.page <= 1;
    nextPageBtn.disabled = data.page >= data.total_pages;
    
    // Update page numbers
    const maxPages = 5;
    let startPage = Math.max(1, data.page - Math.floor(maxPages / 2));
    let endPage = Math.min(data.total_pages, startPage + maxPages - 1);
    if (endPage - startPage < maxPages - 1) {
        startPage = Math.max(1, endPage - maxPages + 1);
    }
    
    pageNumbers.innerHTML = '';
    for (let i = startPage; i <= endPage; i++) {
        const pageBtn = document.createElement('button');
        pageBtn.className = `page-btn ${i === data.page ? 'active' : ''}`;
        pageBtn.textContent = i;
        pageBtn.onclick = () => goToPage(i);
        pageNumbers.appendChild(pageBtn);
    }
}

// Pagination handlers
function goToPage(page) {
    if (page >= 1 && page <= totalPages) {
        currentPage = page;
        loadProducts();
    }
}

prevPageBtn.addEventListener('click', () => goToPage(currentPage - 1));
nextPageBtn.addEventListener('click', () => goToPage(currentPage + 1));

// Filter handlers with debouncing
function applyFilters() {
    currentFilters = {};
    
    if (filterSku.value.trim()) currentFilters.sku = filterSku.value.trim();
    if (filterName.value.trim()) currentFilters.name = filterName.value.trim();
    if (filterDescription.value.trim()) currentFilters.description = filterDescription.value.trim();
    if (filterActive.value) currentFilters.active = filterActive.value === 'true';
    
    currentPage = 1; // Reset to first page when filtering
    loadProducts();
}

filterSku.addEventListener('input', () => {
    clearTimeout(filterTimeout);
    filterTimeout = setTimeout(applyFilters, 500);
});

filterName.addEventListener('input', () => {
    clearTimeout(filterTimeout);
    filterTimeout = setTimeout(applyFilters, 500);
});

filterDescription.addEventListener('input', () => {
    clearTimeout(filterTimeout);
    filterTimeout = setTimeout(applyFilters, 500);
});

filterActive.addEventListener('change', applyFilters);

clearFiltersBtn.addEventListener('click', () => {
    filterSku.value = '';
    filterName.value = '';
    filterDescription.value = '';
    filterActive.value = '';
    applyFilters();
});

// Modal handlers
createProductBtn.addEventListener('click', () => {
    editingProductId = null;
    modalTitle.textContent = 'Create Product';
    productForm.reset();
    document.getElementById('product-active').checked = true;
    clearFormErrors();
    productModal.classList.remove('hidden');
});

closeModalBtn.addEventListener('click', closeProductModal);
cancelProductBtn.addEventListener('click', closeProductModal);

function closeProductModal() {
    productModal.classList.add('hidden');
    editingProductId = null;
    productForm.reset();
    clearFormErrors();
}

// Form submission
productForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const formData = {
        sku: document.getElementById('product-sku').value.trim(),
        name: document.getElementById('product-name').value.trim(),
        description: document.getElementById('product-description').value.trim(),
        active: document.getElementById('product-active').checked
    };
    
    clearFormErrors();
    
    try {
        const url = editingProductId 
            ? `${API_BASE}/products/${editingProductId}`
            : `${API_BASE}/products`;
        
        const method = editingProductId ? 'PUT' : 'POST';
        
        const response = await fetch(url, {
            method,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(formData)
        });
        
        const data = await response.json();
        
        if (!response.ok) {
            if (response.status === 400) {
                showFormError('sku-error', data.detail || 'Validation error');
            } else {
                throw new Error(data.detail || 'Failed to save product');
            }
            return;
        }
        
        closeProductModal();
        loadProducts();
        showNotification('Product saved successfully!', 'success');
        
    } catch (error) {
        console.error('Error saving product:', error);
        showFormError('sku-error', error.message);
    }
});

// Edit product
async function editProduct(id) {
    try {
        const response = await fetch(`${API_BASE}/products/${id}`);
        if (!response.ok) throw new Error('Failed to fetch product');
        
        const product = await response.json();
        
        editingProductId = id;
        modalTitle.textContent = 'Edit Product';
        document.getElementById('product-sku').value = product.sku;
        document.getElementById('product-name').value = product.name;
        document.getElementById('product-description').value = product.description || '';
        document.getElementById('product-active').checked = product.active;
        clearFormErrors();
        productModal.classList.remove('hidden');
        
    } catch (error) {
        console.error('Error loading product:', error);
        showNotification('Failed to load product', 'error');
    }
}

// Delete product
function deleteProduct(id) {
    showConfirmModal(
        'Delete Product',
        'Are you sure you want to delete this product? This action cannot be undone.',
        async () => {
            try {
                const response = await fetch(`${API_BASE}/products/${id}`, {
                    method: 'DELETE'
                });
                
                if (!response.ok) throw new Error('Failed to delete product');
                
                showNotification('Product deleted successfully!', 'success');
                loadProducts();
                
            } catch (error) {
                console.error('Error deleting product:', error);
                showNotification('Failed to delete product', 'error');
            }
        }
    );
}

// Bulk delete
bulkDeleteBtn.addEventListener('click', () => {
    showConfirmModal(
        'Bulk Delete Products',
        'Are you sure you want to delete ALL products? This action cannot be undone. This will delete all products regardless of filters.',
        async () => {
            try {
                const response = await fetch(`${API_BASE}/products/bulk?confirm=true`, {
                    method: 'DELETE'
                });
                
                if (!response.ok) throw new Error('Failed to delete products');
                
                const data = await response.json();
                showNotification(`Successfully deleted ${data.deleted_count} product(s)!`, 'success');
                loadProducts();
                
            } catch (error) {
                console.error('Error deleting products:', error);
                showNotification('Failed to delete products', 'error');
            }
        }
    );
});

// Confirmation modal
function showConfirmModal(title, message, onConfirm) {
    document.getElementById('confirm-title').textContent = title;
    confirmMessage.textContent = message;
    confirmModal.classList.remove('hidden');
    
    const handleConfirm = () => {
        confirmModal.classList.add('hidden');
        onConfirm();
        confirmActionBtn.removeEventListener('click', handleConfirm);
    };
    
    confirmActionBtn.onclick = handleConfirm;
}

closeConfirmBtn.addEventListener('click', () => confirmModal.classList.add('hidden'));
cancelConfirmBtn.addEventListener('click', () => confirmModal.classList.add('hidden'));

// Utility functions
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function clearFormErrors() {
    document.querySelectorAll('.error-message').forEach(el => {
        el.textContent = '';
        el.style.display = 'none';
    });
}

function showFormError(fieldId, message) {
    const errorEl = document.getElementById(fieldId);
    if (errorEl) {
        errorEl.textContent = message;
        errorEl.style.display = 'block';
    }
}

function showNotification(message, type = 'info') {
    // Simple notification - you can enhance this with a toast library
    alert(message);
}

// ==================== WEBHOOK MANAGEMENT ====================

// Webhook state
let editingWebhookId = null;

// DOM Elements for Webhooks
const webhooksTbody = document.getElementById('webhooks-tbody');
const createWebhookBtn = document.getElementById('create-webhook-btn');
const webhookModal = document.getElementById('webhook-modal');
const webhookForm = document.getElementById('webhook-form');
const webhookModalTitle = document.getElementById('webhook-modal-title');
const closeWebhookModalBtn = document.getElementById('close-webhook-modal-btn');
const cancelWebhookBtn = document.getElementById('cancel-webhook-btn');

// Load webhooks from API
async function loadWebhooks() {
    try {
        webhooksTbody.innerHTML = '<tr><td colspan="7" class="loading">Loading webhooks...</td></tr>';
        
        const response = await fetch(`${API_BASE}/webhooks`);
        if (!response.ok) throw new Error('Failed to fetch webhooks');
        
        const webhooks = await response.json();
        renderWebhooks(webhooks);
        
    } catch (error) {
        console.error('Error loading webhooks:', error);
        webhooksTbody.innerHTML = `<tr><td colspan="7" class="error">Error loading webhooks: ${error.message}</td></tr>`;
    }
}

// Render webhooks table
function renderWebhooks(webhooks) {
    if (webhooks.length === 0) {
        webhooksTbody.innerHTML = '<tr><td colspan="7" class="empty">No webhooks found. Create one to get started!</td></tr>';
        return;
    }
    
    webhooksTbody.innerHTML = webhooks.map(webhook => `
        <tr>
            <td>${webhook.id}</td>
            <td class="url-cell">${escapeHtml(webhook.url)}</td>
            <td>
                <div class="event-types">
                    ${webhook.event_types.map(event => `<span class="event-badge">${event}</span>`).join('')}
                </div>
            </td>
            <td>
                <span class="badge ${webhook.enabled ? 'badge-success' : 'badge-inactive'}">
                    ${webhook.enabled ? 'Enabled' : 'Disabled'}
                </span>
            </td>
            <td>${webhook.timeout}s</td>
            <td>${webhook.retry_count}</td>
            <td class="actions">
                <button class="btn-icon btn-edit" onclick="editWebhook(${webhook.id})" title="Edit">✏️</button>
                <button class="btn-icon btn-delete" onclick="deleteWebhook(${webhook.id})" title="Delete">🗑️</button>
            </td>
        </tr>
    `).join('');
}

// Modal handlers
createWebhookBtn.addEventListener('click', () => {
    editingWebhookId = null;
    webhookModalTitle.textContent = 'Create Webhook';
    webhookForm.reset();
    document.getElementById('webhook-enabled').checked = true;
    document.getElementById('webhook-timeout').value = 30;
    document.getElementById('webhook-retry-count').value = 3;
    clearFormErrors();
    webhookModal.classList.remove('hidden');
});

closeWebhookModalBtn.addEventListener('click', closeWebhookModal);
cancelWebhookBtn.addEventListener('click', closeWebhookModal);

function closeWebhookModal() {
    webhookModal.classList.add('hidden');
    editingWebhookId = null;
    webhookForm.reset();
    clearFormErrors();
}

// Form submission
webhookForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    
    // Get event types from checkboxes
    const eventTypes = Array.from(document.querySelectorAll('input[name="event_types"]:checked'))
        .map(cb => cb.value);
    
    if (eventTypes.length === 0) {
        showFormError('event-types-error', 'At least one event type must be selected');
        return;
    }
    
    const formData = {
        url: document.getElementById('webhook-url').value.trim(),
        event_types: eventTypes,
        enabled: document.getElementById('webhook-enabled').checked,
        timeout: parseInt(document.getElementById('webhook-timeout').value),
        retry_count: parseInt(document.getElementById('webhook-retry-count').value)
    };
    
    const secret = document.getElementById('webhook-secret').value.trim();
    if (secret) {
        formData.secret = secret;
    }
    
    clearFormErrors();
    
    try {
        const url = editingWebhookId 
            ? `${API_BASE}/webhooks/${editingWebhookId}`
            : `${API_BASE}/webhooks`;
        
        const method = editingWebhookId ? 'PUT' : 'POST';
        
        const response = await fetch(url, {
            method,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(formData)
        });
        
        const data = await response.json();
        
        if (!response.ok) {
            if (response.status === 400 || response.status === 422) {
                const errorMsg = Array.isArray(data.detail) 
                    ? data.detail.map(e => e.msg).join(', ')
                    : data.detail;
                showFormError('url-error', errorMsg);
            } else {
                throw new Error(data.detail || 'Failed to save webhook');
            }
            return;
        }
        
        closeWebhookModal();
        loadWebhooks();
        showNotification('Webhook saved successfully!', 'success');
        
    } catch (error) {
        console.error('Error saving webhook:', error);
        showFormError('url-error', error.message);
    }
});

// Edit webhook
async function editWebhook(id) {
    try {
        const response = await fetch(`${API_BASE}/webhooks/${id}`);
        if (!response.ok) throw new Error('Failed to fetch webhook');
        
        const webhook = await response.json();
        
        editingWebhookId = id;
        webhookModalTitle.textContent = 'Edit Webhook';
        document.getElementById('webhook-url').value = webhook.url;
        document.getElementById('webhook-enabled').checked = webhook.enabled;
        document.getElementById('webhook-secret').value = webhook.secret || '';
        document.getElementById('webhook-timeout').value = webhook.timeout;
        document.getElementById('webhook-retry-count').value = webhook.retry_count;
        
        // Set event type checkboxes
        document.querySelectorAll('input[name="event_types"]').forEach(cb => {
            cb.checked = webhook.event_types.includes(cb.value);
        });
        
        clearFormErrors();
        webhookModal.classList.remove('hidden');
        
    } catch (error) {
        console.error('Error loading webhook:', error);
        showNotification('Failed to load webhook', 'error');
    }
}

// Delete webhook
function deleteWebhook(id) {
    showConfirmModal(
        'Delete Webhook',
        'Are you sure you want to delete this webhook? This action cannot be undone.',
        async () => {
            try {
                const response = await fetch(`${API_BASE}/webhooks/${id}`, {
                    method: 'DELETE'
                });
                
                if (!response.ok) throw new Error('Failed to delete webhook');
                
                showNotification('Webhook deleted successfully!', 'success');
                loadWebhooks();
                
            } catch (error) {
                console.error('Error deleting webhook:', error);
                showNotification('Failed to delete webhook', 'error');
            }
        }
    );
}


