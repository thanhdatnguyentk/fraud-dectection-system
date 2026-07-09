// FDS Dashboard - Client Script

document.addEventListener("DOMContentLoaded", () => {
    initNavigation();
    initChart();
    initImportanceChart();
    initWebSocket();
});

let liveChart;
let importChart;

function initNavigation() {
    const navItems = document.querySelectorAll('.sidebar nav .nav-item[data-target]');
    const views = document.querySelectorAll('.view-section');
    const pageTitle = document.getElementById('page-title');

    navItems.forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            
            // Update active state in sidebar
            navItems.forEach(nav => nav.classList.remove('active'));
            item.classList.add('active');
            
            // Hide all views
            views.forEach(view => view.style.display = 'none');
            
            // Show target view
            const targetId = item.getAttribute('data-target');
            document.getElementById(targetId).style.display = 'block';
            
            // Update page title
            pageTitle.innerText = item.innerText;
        });
    });
}

function initChart() {
    const ctx = document.getElementById('liveChart').getContext('2d');
    
    // Gradient for the line area
    const gradient = ctx.createLinearGradient(0, 0, 0, 400);
    gradient.addColorStop(0, 'rgba(0, 240, 255, 0.2)');
    gradient.addColorStop(1, 'rgba(0, 240, 255, 0)');

    // Mock initial data
    const labels = Array.from({length: 20}, (_, i) => `-${20-i}s`);
    const dataVol = Array.from({length: 20}, () => Math.floor(Math.random() * 50) + 150);

    liveChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Transactions / sec',
                data: dataVol,
                borderColor: '#00f0ff',
                backgroundColor: gradient,
                borderWidth: 2,
                pointRadius: 0,
                pointHoverRadius: 6,
                fill: true,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    backgroundColor: 'rgba(10, 10, 15, 0.9)',
                    titleColor: '#8a8a9a',
                    bodyColor: '#e0e0e5',
                    borderColor: 'rgba(255,255,255,0.1)',
                    borderWidth: 1
                }
            },
            scales: {
                x: {
                    grid: {
                        display: false,
                        drawBorder: false
                    },
                    ticks: {
                        color: '#8a8a9a',
                        maxTicksLimit: 10
                    }
                },
                y: {
                    grid: {
                        color: 'rgba(255, 255, 255, 0.05)',
                        drawBorder: false
                    },
                    ticks: {
                        color: '#8a8a9a'
                    },
                    min: 0,
                    max: 300
                }
            },
            animation: {
                duration: 0 // disable animation for live update feel
            }
        }
    });
}

function initImportanceChart() {
    const ctx = document.getElementById('importanceChart');
    if(!ctx) return;
    
    importChart = new Chart(ctx.getContext('2d'), {
        type: 'bar',
        data: {
            labels: ['amt_sum_1h', 'tx_count_10m', 'pct_ecommerce', 'max_amt_1h', 'distinct_mcc_30d', 'avg_seconds_between_tx'],
            datasets: [{
                label: 'Feature Importance (Gain)',
                data: [0.35, 0.22, 0.15, 0.12, 0.09, 0.07],
                backgroundColor: 'rgba(0, 240, 255, 0.6)',
                borderColor: '#00f0ff',
                borderWidth: 1,
                borderRadius: 4
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false }
            },
            scales: {
                x: {
                    grid: { color: 'rgba(255,255,255,0.05)' },
                    ticks: { color: '#8a8a9a' }
                },
                y: {
                    grid: { display: false },
                    ticks: { color: '#e0e0e5' }
                }
            }
        }
    });
}



function initWebSocket() {
    const ws = new WebSocket(`ws://${window.location.hostname}:8001/ws/metrics`);
    
    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        
        // 1. Update Chart & Metrics
        if(liveChart) {
            const currentData = liveChart.data.datasets[0].data;
            currentData.shift();
            currentData.push(data.tps);
            liveChart.update();
            
            const tpsEl = document.getElementById('tps-value');
            if(tpsEl) tpsEl.innerText = data.tps.toFixed(1);
            
            const latEl = document.getElementById('latency-value');
            if(latEl) latEl.innerText = data.latency.toFixed(1) + " ms";
            
            const fraudEl = document.getElementById('fraud-rate');
            if(fraudEl && data.fraud_rate !== undefined) {
                fraudEl.innerText = data.fraud_rate.toFixed(2) + "%";
            }
        }
        
        // 2. Update Stream Table
        if(document.getElementById('view-streaming').style.display !== 'none') {
            const streamBody = document.getElementById('streamTableBody');
            if(streamBody && data.recent_txs) {
                streamBody.innerHTML = data.recent_txs.map(tx => `
                    <tr>
                        <td>${tx.tx_id}</td>
                        <td>${tx.user_id}</td>
                        <td style="color: var(--accent-green)">${tx.amount}</td>
                        <td>${tx.merchant_id}</td>
                        <td style="color: #8a8a9a">${tx.time}</td>
                    </tr>
                `).join('');
            }
        }
        
        // 3. Update Alerts List (Overview)
        const alertsList = document.getElementById('alertsList');
        if(alertsList && data.recent_alerts) {
            alertsList.innerHTML = data.recent_alerts.slice(0, 5).map(a => `
                <div class="alert-item ${a.type}">
                    <div class="alert-header">
                        <span class="alert-id">${a.id}</span>
                        <span class="alert-amount">${a.amount}</span>
                    </div>
                    <div class="alert-rule">${a.rule}</div>
                </div>
            `).join('');
        }
        
        // 4. Update Full Alerts Table
        if(document.getElementById('view-alerts').style.display !== 'none') {
            const fullBody = document.getElementById('fullAlertsBody');
            if(fullBody && data.recent_alerts) {
                fullBody.innerHTML = data.recent_alerts.map(a => `
                    <tr>
                        <td>${a.id}</td>
                        <td><span class="badge-${a.type}">${a.type.toUpperCase()}</span></td>
                        <td>${a.amount}</td>
                        <td style="color: #8a8a9a">${a.rule}</td>
                        <td style="color: #8a8a9a">${a.time}</td>
                    </tr>
                `).join('');
            }
        }
    };
    
    ws.onclose = () => {
        console.log("WebSocket connection lost. Retrying in 5 seconds...");
        setTimeout(initWebSocket, 5000);
    };
}
