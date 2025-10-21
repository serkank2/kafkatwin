// API base URL
const API_BASE = '/api/v1';

// State
let currentView = 'overview';

// Initialize app
document.addEventListener('DOMContentLoaded', () => {
    setupNavigation();
    setupModalHandlers();
    loadOverview();

    // Auto-refresh every 5 seconds
    setInterval(() => {
        if (currentView === 'overview') {
            loadOverview();
        }
    }, 5000);
});

// Navigation
function setupNavigation() {
    const navBtns = document.querySelectorAll('.nav-btn');
    navBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            const view = btn.dataset.view;
            switchView(view);
        });
    });
}

function switchView(view) {
    currentView = view;

    // Update nav buttons
    document.querySelectorAll('.nav-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.view === view);
    });

    // Update views
    document.querySelectorAll('.view-section').forEach(section => {
        section.classList.toggle('active', section.id === view);
    });

    // Load data for view
    switch(view) {
        case 'overview':
            loadOverview();
            break;
        case 'clusters':
            loadClusters();
            break;
        case 'topics':
            loadTopics();
            break;
        case 'transformations':
            loadTransformations();
            break;
        case 'quotas':
            loadQuotas();
            break;
        case 'schemas':
            loadSchemas();
            break;
    }
}

// API calls
async function apiCall(endpoint, options = {}) {
    try {
        const response = await fetch(`${API_BASE}${endpoint}`, options);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        return await response.json();
    } catch (error) {
        console.error('API call failed:', error);
        showError(error.message);
        return null;
    }
}

// Overview
async function loadOverview() {
    const info = await apiCall('/info');
    const clusters = await apiCall('/clusters');

    if (info) {
        document.getElementById('cluster-count').textContent = info.clusters || 0;
        document.getElementById('topic-count').textContent = info.topics || 0;
    }

    if (clusters) {
        const healthyClusters = clusters.filter(c => c.healthy).length;
        const healthStatus = healthyClusters === clusters.length ? 'HEALTHY' :
                           healthyClusters > 0 ? 'DEGRADED' : 'UNHEALTHY';
        document.getElementById('system-health').textContent = healthStatus;
        document.getElementById('system-health').className = `stat health ${healthStatus.toLowerCase()}`;

        renderClusterHealth(clusters);
    }
}

function renderClusterHealth(clusters) {
    const container = document.getElementById('cluster-health-list');
    container.innerHTML = clusters.map(cluster => `
        <div class="list-item">
            <div>
                <div class="list-item-title">${cluster.id}</div>
                <div class="list-item-meta">${cluster.config.bootstrap_servers.join(', ')}</div>
            </div>
            <span class="badge ${cluster.healthy ? 'badge-success' : 'badge-danger'}">
                ${cluster.healthy ? 'Healthy' : 'Unhealthy'}
            </span>
        </div>
    `).join('');
}

// Clusters
async function loadClusters() {
    const clusters = await apiCall('/clusters');
    const container = document.getElementById('cluster-list');

    if (!clusters || clusters.length === 0) {
        container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">🔌</div><p>No clusters configured</p></div>';
        return;
    }

    container.innerHTML = clusters.map(cluster => `
        <div class="list-item">
            <div>
                <div class="list-item-title">${cluster.id}</div>
                <div class="list-item-meta">
                    Brokers: ${cluster.config.bootstrap_servers.join(', ')}<br>
                    Priority: ${cluster.config.priority} | Weight: ${cluster.config.weight}
                </div>
            </div>
            <div>
                <span class="badge ${cluster.healthy ? 'badge-success' : 'badge-danger'}">
                    ${cluster.healthy ? 'Healthy' : 'Unhealthy'}
                </span>
                <button class="btn btn-secondary" onclick="viewClusterDetails('${cluster.id}')">Details</button>
            </div>
        </div>
    `).join('');
}

async function viewClusterDetails(clusterId) {
    const cluster = await apiCall(`/clusters/${clusterId}`);
    if (!cluster) return;

    showModal('Cluster Details', `
        <div class="form-group">
            <label>Cluster ID</label>
            <input type="text" value="${cluster.id}" readonly>
        </div>
        <div class="form-group">
            <label>Bootstrap Servers</label>
            <textarea readonly>${cluster.config.bootstrap_servers.join('\n')}</textarea>
        </div>
        <div class="form-group">
            <label>Health Status</label>
            <input type="text" value="${cluster.healthy ? 'Healthy' : 'Unhealthy'}" readonly>
        </div>
        <div class="form-group">
            <label>Error Count</label>
            <input type="text" value="${cluster.health?.error_count || 0}" readonly>
        </div>
        <div class="form-group">
            <label>Success Count</label>
            <input type="text" value="${cluster.health?.success_count || 0}" readonly>
        </div>
    `, null, true);
}

// Topics
async function loadTopics() {
    const topics = await apiCall('/topics');
    const container = document.getElementById('topic-list');

    if (!topics || topics.length === 0) {
        container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">📁</div><p>No topics found</p></div>';
        return;
    }

    container.innerHTML = topics.map(topic => `
        <div class="list-item">
            <div>
                <div class="list-item-title">${topic}</div>
            </div>
            <button class="btn btn-secondary" onclick="viewTopicDetails('${topic}')">Details</button>
        </div>
    `).join('');
}

async function viewTopicDetails(topic) {
    const details = await apiCall(`/topics/${topic}`);
    if (!details) return;

    const partitionCount = Object.keys(details.partitions || {}).length;

    showModal('Topic Details', `
        <div class="form-group">
            <label>Topic Name</label>
            <input type="text" value="${details.name}" readonly>
        </div>
        <div class="form-group">
            <label>Partitions</label>
            <input type="text" value="${partitionCount}" readonly>
        </div>
        <div class="form-group">
            <label>Clusters</label>
            <textarea readonly>${(details.cluster_ids || []).join('\n')}</textarea>
        </div>
    `, null, true);
}

// Transformations
async function loadTransformations() {
    const transformations = await apiCall('/transformations');
    const container = document.getElementById('transformation-list');

    if (!transformations || Object.keys(transformations).length === 0) {
        container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">🔄</div><p>No transformation rules defined</p></div>';
        return;
    }

    let html = '';
    for (const [topic, rules] of Object.entries(transformations)) {
        html += `<h4>${topic}</h4>`;
        html += rules.map(rule => `
            <div class="list-item">
                <div>
                    <div class="list-item-title">${rule.name}</div>
                    <div class="list-item-meta">
                        Actions: ${rule.actions.length} | Priority: ${rule.priority}
                    </div>
                </div>
                <div>
                    <span class="badge ${rule.enabled ? 'badge-success' : 'badge-warning'}">
                        ${rule.enabled ? 'Enabled' : 'Disabled'}
                    </span>
                    <button class="btn btn-danger" onclick="deleteTransformation('${topic}', '${rule.id}')">Delete</button>
                </div>
            </div>
        `).join('');
    }

    container.innerHTML = html;
}

async function deleteTransformation(topic, ruleId) {
    if (!confirm('Are you sure you want to delete this transformation rule?')) return;

    await apiCall(`/transformations/${topic}/${ruleId}`, { method: 'DELETE' });
    loadTransformations();
}

// Quotas
async function loadQuotas() {
    const quotas = await apiCall('/quotas');
    const container = document.getElementById('quota-list');

    if (!quotas || Object.keys(quotas).length === 0) {
        container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">⚡</div><p>No quotas configured</p></div>';
        return;
    }

    container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">⚡</div><p>Quotas feature coming soon</p></div>';
}

// Schemas
async function loadSchemas() {
    const subjects = await apiCall('/schemas/subjects');
    const container = document.getElementById('schema-list');

    if (!subjects || subjects.length === 0) {
        container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">📋</div><p>No schemas registered</p></div>';
        return;
    }

    container.innerHTML = subjects.map(subject => `
        <div class="list-item">
            <div>
                <div class="list-item-title">${subject}</div>
            </div>
            <div>
                <button class="btn btn-secondary" onclick="viewSchema('${subject}')">View</button>
                <button class="btn btn-danger" onclick="deleteSchema('${subject}')">Delete</button>
            </div>
        </div>
    `).join('');
}

async function viewSchema(subject) {
    const schema = await apiCall(`/schemas/subjects/${subject}/versions/latest`);
    if (!schema) return;

    showModal('Schema Details', `
        <div class="form-group">
            <label>Subject</label>
            <input type="text" value="${schema.subject}" readonly>
        </div>
        <div class="form-group">
            <label>Version</label>
            <input type="text" value="${schema.version}" readonly>
        </div>
        <div class="form-group">
            <label>Schema ID</label>
            <input type="text" value="${schema.id}" readonly>
        </div>
        <div class="form-group">
            <label>Schema Type</label>
            <input type="text" value="${schema.schemaType}" readonly>
        </div>
        <div class="form-group">
            <label>Schema</label>
            <textarea readonly style="min-height: 200px;">${schema.schema}</textarea>
        </div>
    `, null, true);
}

async function deleteSchema(subject) {
    if (!confirm(`Are you sure you want to delete schema subject "${subject}"?`)) return;

    await apiCall(`/schemas/subjects/${subject}`, { method: 'DELETE' });
    loadSchemas();
}

// Modal
function setupModalHandlers() {
    const overlay = document.getElementById('modal-overlay');
    const closeBtns = document.querySelectorAll('.modal-close');

    closeBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            overlay.classList.remove('active');
        });
    });

    overlay.addEventListener('click', (e) => {
        if (e.target === overlay) {
            overlay.classList.remove('active');
        }
    });
}

function showModal(title, body, onSubmit = null, hideSubmit = false) {
    const overlay = document.getElementById('modal-overlay');
    document.getElementById('modal-title').textContent = title;
    document.getElementById('modal-body').innerHTML = body;

    const submitBtn = document.getElementById('modal-submit');
    submitBtn.style.display = hideSubmit ? 'none' : 'block';

    if (onSubmit) {
        submitBtn.onclick = () => {
            onSubmit();
            overlay.classList.remove('active');
        };
    }

    overlay.classList.add('active');
}

function showError(message) {
    alert('Error: ' + message);
}
