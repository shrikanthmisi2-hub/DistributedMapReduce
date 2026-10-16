/**
 * Distributed MapReduce Dashboard JavaScript
 * Handles real-time polling of /api/summary, dynamic DOM rendering,
 * interactive job submissions, result visualization, and historical job execution logs.
 */

// Configuration
const POLL_INTERVAL_MS = 1500;
let currentFilter = 'all';
let pollTimer = null;
let lastTasksData = [];
let currentResultsData = null;
let currentResultsFilter = '';
let isSubmitting = false;
let currentSummaryData = null;
let selectedTimelinePhase = 'ready';

const TIMELINE_PHASES = [
    {
        id: 'ready',
        title: 'Workers Ready & Cluster State',
        desc: 'Master node initialized on TCP port 5000 and confirmed worker node registrations with heartbeat monitoring.'
    },
    {
        id: 'submitted',
        title: 'Job Submitted & Validated',
        desc: 'Input text/file received, validated, encoded to UTF-8, and partitioned into parallel map tasks by the scheduler.'
    },
    {
        id: 'map',
        title: 'Map Phase & Task Execution',
        desc: 'Map tasks dispatched to registered workers over TCP sockets. Workers parse text lines and produce (word, 1) key-value pairs.'
    },
    {
        id: 'shuffle',
        title: 'Shuffle & Group Intermediate Keys',
        desc: 'Master aggregates all intermediate map results by word key into partitioned lists for reduction.'
    },
    {
        id: 'reduce',
        title: 'Reduce Phase & Frequency Calculation',
        desc: 'Reducers aggregate occurrences per word, calculate relative frequencies, and write sorted output files.'
    },
    {
        id: 'complete',
        title: 'Job Completed & Results Available',
        desc: 'Workflow finished successfully. Word count frequencies, total words, and execution metrics available for inspection.'
    }
];

// DOM Element References
const connectionPill = document.getElementById('connection-pill');
const connectionText = document.getElementById('connection-text');
const lastUpdatedEl = document.getElementById('last-updated');
const errorBanner = document.getElementById('error-banner');
const errorMessageEl = document.getElementById('error-message');
const refreshBtn = document.getElementById('refresh-btn');

// Metric Elements
const jobStateBadge = document.getElementById('job-state-badge');
const jobStateSubtext = document.getElementById('job-state-subtext');
const currentJobIdText = document.getElementById('current-job-id-text');
const metricProgressPct = document.getElementById('metric-progress-pct');
const metricProgressSubtext = document.getElementById('metric-progress-subtext');
const metricTotalJobsRun = document.getElementById('metric-total-jobs-run');
const metricTotalTasks = document.getElementById('metric-total-tasks');
const metricTasksSubtext = document.getElementById('metric-tasks-subtext');
const metricCompletedCount = document.getElementById('metric-completed-count');
const metricWorkerCount = document.getElementById('metric-worker-count');
const metricWorkersSubtext = document.getElementById('metric-workers-subtext');

// Progress Bar & Pipeline
const progressBarFill = document.getElementById('progress-bar-fill');
const progressSummaryBadge = document.getElementById('progress-summary-badge');
const jobDurationBadge = document.getElementById('job-duration-badge');
const stepRegistered = document.getElementById('step-registered');
const stepSubmitted = document.getElementById('step-submitted');
const stepMap = document.getElementById('step-map');
const stepShuffle = document.getElementById('step-shuffle');
const stepReduce = document.getElementById('step-reduce');
const stepDone = document.getElementById('step-done');

// Form Elements
const jobSubmitForm = document.getElementById('job-submit-form');
const modeTabText = document.getElementById('mode-tab-text');
const modeTabFile = document.getElementById('mode-tab-file');
const textInputContainer = document.getElementById('text-input-container');
const fileInputContainer = document.getElementById('file-input-container');
const jobInputText = document.getElementById('job-input-text');
const textInputStats = document.getElementById('text-input-stats');
const jobFileInput = document.getElementById('job-file-input');
const fileDropArea = document.getElementById('file-drop-area');
const selectedFileBadge = document.getElementById('selected-file-badge');
const selectedFileName = document.getElementById('selected-file-name');
const removeFileBtn = document.getElementById('remove-file-btn');
const jobNumMappers = document.getElementById('job-num-mappers');
const numMappersVal = document.getElementById('num-mappers-val');
const presetPills = document.querySelectorAll('.preset-pill');
const clearFormBtn = document.getElementById('clear-form-btn');
const loadSampleBtn = document.getElementById('load-sample-btn');
const submitJobBtn = document.getElementById('submit-job-btn');
const submitSpinner = document.getElementById('submit-spinner');
const submitIcon = document.getElementById('submit-icon');
const submitBtnText = document.getElementById('submit-btn-text');
const submitFeedback = document.getElementById('submit-feedback');
const submitFeedbackMsg = document.getElementById('submit-feedback-msg');
const feedbackIcon = document.getElementById('feedback-icon');

// Results Panel Elements
const resultsSection = document.getElementById('results-section');
const resultsJobIdBadge = document.getElementById('results-job-id-badge');
const resultsMetaTimestamp = document.getElementById('results-meta-timestamp');
const resUniqueWords = document.getElementById('res-unique-words');
const resTotalWords = document.getElementById('res-total-words');
const resInputSize = document.getElementById('res-input-size');
const resMapTasks = document.getElementById('res-map-tasks');
const resDuration = document.getElementById('res-duration');
const resStatus = document.getElementById('res-status');
const resultsSearchInput = document.getElementById('results-search-input');
const resultsFilterCount = document.getElementById('results-filter-count');
const resultsTableBody = document.getElementById('results-table-body');

// Workers & Tasks Tables
const workersCountBadge = document.getElementById('workers-count-badge');
const workersTableBody = document.getElementById('workers-table-body');
const tasksCountBadge = document.getElementById('tasks-count-badge');
const tasksTableBody = document.getElementById('tasks-table-body');

// Filter count elements
const filterCountAll = document.getElementById('filter-count-all');
const filterCountPending = document.getElementById('filter-count-pending');
const filterCountAssigned = document.getElementById('filter-count-assigned');
const filterCountCompleted = document.getElementById('filter-count-completed');
const filterCountFailed = document.getElementById('filter-count-failed');

// History Table
const historyCountBadge = document.getElementById('history-count-badge');
const historyTableBody = document.getElementById('history-table-body');

/**
 * Format uptime / duration seconds to human-readable string (e.g., 2m 14s, 1.2s)
 */
function formatDuration(seconds) {
    if (seconds === null || seconds === undefined || isNaN(seconds)) return '--';
    const num = parseFloat(seconds);
    if (num < 1) return `${num.toFixed(2)}s`;
    if (num < 60) return `${num.toFixed(1)}s`;
    const m = Math.floor(num / 60);
    const remS = Math.floor(num % 60);
    return `${m}m ${remS}s`;
}

/**
 * Format byte count to human-readable units (e.g. 1.2 KB)
 */
function formatBytes(bytes) {
    if (!bytes || isNaN(bytes) || bytes <= 0) return '0 B';
    const b = parseInt(bytes, 10);
    if (b < 1024) return `${b} B`;
    if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`;
    return `${(b / (1024 * 1024)).toFixed(2)} MB`;
}

/**
 * Update connection pill and error banner UI
 */
function setConnectionState(online, message = '') {
    if (online) {
        connectionPill.className = 'status-pill online';
        connectionText.textContent = 'Master Online';
        errorBanner.classList.add('hidden');
    } else {
        connectionPill.className = 'status-pill offline';
        connectionText.textContent = 'Master Offline';
        errorMessageEl.textContent = message || 'Unable to connect to Master API on port 8080. Retrying...';
        errorBanner.classList.remove('hidden');
    }
}

/**
 * Update Job Status Cards, Progress Bar, and Stepper
 */
function updateJobStatus(status, workers, tasks) {
    const stats = status.stats || {};
    const total = stats.total || 0;
    const completed = stats.completed || 0;
    const pending = stats.pending || 0;
    const assigned = stats.assigned || 0;
    const failed = stats.failed || 0;

    // Current Job ID display
    const activeJobId = status.job_id || '—';
    if (currentJobIdText) {
        currentJobIdText.textContent = activeJobId;
    }

    // Total jobs count
    if (metricTotalJobsRun) {
        metricTotalJobsRun.textContent = status.total_jobs_submitted || 0;
    }
    if (metricCompletedCount) {
        metricCompletedCount.textContent = completed;
    }

    // Calculate completion percentage
    const pct = total > 0 ? Math.round((completed / total) * 100) : 0;
    metricProgressPct.textContent = `${pct}%`;
    metricProgressSubtext.textContent = `${completed} / ${total} tasks completed`;
    progressBarFill.style.width = `${pct}%`;
    progressSummaryBadge.textContent = `${completed} / ${total} Completed`;

    // Duration badge
    if (jobDurationBadge) {
        if (status.duration_seconds !== null && status.duration_seconds !== undefined) {
            jobDurationBadge.textContent = `Duration: ${formatDuration(status.duration_seconds)}`;
        } else if (status.job_in_progress) {
            jobDurationBadge.textContent = 'Executing...';
        } else {
            jobDurationBadge.textContent = 'Duration: 0.0s';
        }
    }

    // Determine Job State Badge & Subtext
    let stateLabel = 'IDLE';
    let stateClass = 'badge-idle';
    let subtext = 'Waiting for job submission';

    const stage = (status.current_stage || '').toUpperCase();

    if (!status.server_running) {
        stateLabel = 'OFFLINE';
        stateClass = 'badge-failed';
        subtext = 'Master server is offline';
    } else if (status.job_failed) {
        stateLabel = 'FAILED';
        stateClass = 'badge-failed';
        subtext = status.job_error ? `Error: ${status.job_error}` : 'Job encountered unrecoverable task errors';
    } else if (stage === 'SHUFFLE_AND_REDUCE' || (status.job_in_progress && completed === total && total > 0)) {
        stateLabel = 'SHUFFLE & REDUCE';
        stateClass = 'badge-running';
        subtext = 'Aggregating map outputs & sorting';
    } else if (stage === 'MAPPING' || status.job_in_progress) {
        stateLabel = 'MAPPING';
        stateClass = 'badge-running';
        subtext = `Executing map tasks (${assigned} in flight, ${pending} queued)`;
    } else if (status.job_completed || stage === 'COMPLETED') {
        stateLabel = 'COMPLETED';
        stateClass = 'badge-completed';
        subtext = `MapReduce job completed in ${formatDuration(status.duration_seconds)}`;
    } else if (workers.length > 0) {
        stateLabel = 'CLUSTER READY';
        stateClass = 'badge-idle';
        subtext = `${workers.length} worker(s) registered. Ready for jobs.`;
    }

    jobStateBadge.textContent = stateLabel;
    jobStateBadge.className = `state-badge ${stateClass}`;
    jobStateSubtext.textContent = subtext;

    // Task Queue Card
    metricTotalTasks.textContent = total;
    metricTasksSubtext.textContent = `Pending: ${pending} · Assigned: ${assigned} · Failed: ${failed}`;

    // Active Workers Card
    const idleCount = workers.filter(w => w.status === 'IDLE').length;
    const busyCount = workers.filter(w => w.status === 'BUSY').length;
    const unavailCount = workers.filter(w => w.status === 'UNAVAILABLE').length;
    metricWorkerCount.textContent = workers.length;
    metricWorkersSubtext.textContent = `${idleCount} Idle · ${busyCount} Busy · ${unavailCount} Unavailable`;

    // Pipeline stepper & timeline updated in renderExecutionTimeline

    // Synchronize Submit Button Disabled State
    if (!isSubmitting) {
        if (status.job_in_progress) {
            submitJobBtn.disabled = true;
            submitBtnText.textContent = 'Job Running...';
            submitSpinner.classList.remove('hidden');
            submitIcon.classList.add('hidden');
        } else {
            submitJobBtn.disabled = false;
            submitBtnText.textContent = 'Run MapReduce Job';
            submitSpinner.classList.add('hidden');
            submitIcon.classList.remove('hidden');
        }
    }
}

/**
 * Render Workers Table
 */
function renderWorkers(workers) {
    workersCountBadge.textContent = workers.length;

    if (!workers || workers.length === 0) {
        workersTableBody.innerHTML = `
            <tr class="empty-row">
                <td colspan="6">
                    <div class="empty-state">
                        <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                            <rect x="2" y="2" width="20" height="8" rx="2" ry="2"></rect>
                            <rect x="2" y="14" width="20" height="8" rx="2" ry="2"></rect>
                            <line x1="6" y1="6" x2="6.01" y2="6"></line>
                            <line x1="6" y1="18" x2="6.01" y2="18"></line>
                        </svg>
                        <p>No workers currently registered</p>
                        <span class="empty-hint">Start a worker with <code>python workers/worker.py --worker-id worker-1</code></span>
                    </div>
                </td>
            </tr>
        `;
        return;
    }

    const rows = workers.map(worker => {
        const wid = worker.worker_id || 'unknown';
        const status = worker.status || 'UNAVAILABLE';
        
        let statusBadgeClass = 'status-unavailable';
        if (status === 'IDLE') statusBadgeClass = 'status-idle';
        else if (status === 'BUSY') statusBadgeClass = 'status-busy';

        const currentTask = (worker.current_task && worker.current_task !== '--' && worker.current_task !== '—') 
            ? `<span class="code-pill">${worker.current_task}</span>` 
            : '<span class="text-muted">--</span>';

        const hbSec = worker.seconds_since_heartbeat;
        let hbClass = 'heartbeat-green';
        let hbText = '--';
        if (hbSec !== null && hbSec !== undefined) {
            hbText = `${hbSec.toFixed(1)}s ago`;
            if (hbSec > 3.0) hbClass = 'heartbeat-red';
            else if (hbSec > 1.5) hbClass = 'heartbeat-yellow';
        }

        const address = worker.address || '--';
        const uptime = formatDuration(worker.uptime_seconds);

        return `
            <tr>
                <td><span class="code-pill">${wid}</span></td>
                <td>
                    <span class="badge-status ${statusBadgeClass}">
                        <span class="pulse-dot"></span>
                        ${status}
                    </span>
                </td>
                <td>${currentTask}</td>
                <td>
                    <div class="heartbeat-cell">
                        <span class="heartbeat-indicator ${hbClass}"></span>
                        <span>${hbText}</span>
                    </div>
                </td>
                <td><span class="font-mono text-muted">${address}</span></td>
                <td><span class="font-mono text-muted">${uptime}</span></td>
            </tr>
        `;
    });

    workersTableBody.innerHTML = rows.join('');
}

/**
 * Render Tasks Table with Filtering
 */
function renderTasks(tasks) {
    lastTasksData = tasks || [];
    tasksCountBadge.textContent = lastTasksData.length;

    // Update filter counts
    const counts = { all: lastTasksData.length, pending: 0, assigned: 0, completed: 0, failed: 0 };
    lastTasksData.forEach(t => {
        const st = (t.state || '').toLowerCase();
        if (counts[st] !== undefined) counts[st]++;
    });

    filterCountAll.textContent = counts.all;
    filterCountPending.textContent = counts.pending;
    filterCountAssigned.textContent = counts.assigned;
    filterCountCompleted.textContent = counts.completed;
    filterCountFailed.textContent = counts.failed;

    // Filter tasks
    const filtered = currentFilter === 'all' 
        ? lastTasksData 
        : lastTasksData.filter(t => (t.state || '').toLowerCase() === currentFilter);

    if (filtered.length === 0) {
        tasksTableBody.innerHTML = `
            <tr class="empty-row">
                <td colspan="5">
                    <div class="empty-state">
                        <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                            <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path>
                            <polyline points="14 2 14 8 20 8"></polyline>
                            <line x1="16" y1="13" x2="8" y2="13"></line>
                            <line x1="16" y1="17" x2="8" y2="17"></line>
                            <polyline points="10 9 9 9 8 9"></polyline>
                        </svg>
                        <p>${lastTasksData.length === 0 ? 'No tasks scheduled yet' : `No tasks in '${currentFilter}' state`}</p>
                        <span class="empty-hint">Submit a job above to schedule tasks across workers</span>
                    </div>
                </td>
            </tr>
        `;
        return;
    }

    const rows = filtered.map(t => {
        const tid = t.task_id || 'unknown';
        const state = (t.state || 'pending').toLowerCase();
        const stateClass = `task-state-${state}`;

        const workerBadge = t.assigned_worker 
            ? `<span class="code-pill">${t.assigned_worker}</span>` 
            : '<span class="text-muted">Unassigned</span>';

        const retries = `${t.retries || 0} / ${t.max_retries || 3}`;
        const reason = (t.failure_reason && t.failure_reason !== '--' && t.failure_reason !== '—') 
            ? `<span class="text-rose font-mono">${t.failure_reason}</span>` 
            : '<span class="text-muted">--</span>';

        return `
            <tr>
                <td><span class="code-pill">${tid}</span></td>
                <td><span class="badge-task-state ${stateClass}">${state}</span></td>
                <td>${workerBadge}</td>
                <td><span class="font-mono">${retries}</span></td>
                <td>${reason}</td>
            </tr>
        `;
    });

    tasksTableBody.innerHTML = rows.join('');
}

/**
 * Render MapReduce Results Panel
 */
function renderResults(resultsData) {
    currentResultsData = resultsData;

    if (!resultsData || !resultsData.results || resultsData.results.length === 0) {
        resultsJobIdBadge.textContent = 'No Output Available';
        resultsJobIdBadge.className = 'pill-neutral font-mono';
        resultsMetaTimestamp.textContent = 'Awaiting job completion';
        resUniqueWords.textContent = '0';
        resTotalWords.textContent = '0';
        resInputSize.textContent = '0 B';
        resMapTasks.textContent = '0';
        resDuration.textContent = '0.0s';
        resStatus.textContent = 'IDLE';
        resStatus.className = 'state-badge badge-idle';
        resultsFilterCount.textContent = '0 results';

        resultsTableBody.innerHTML = `
            <tr class="empty-row" id="results-empty-row">
                <td colspan="4">
                    <div class="empty-state">
                        <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                            <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path>
                            <polyline points="14 2 14 8 20 8"></polyline>
                            <line x1="16" y1="13" x2="8" y2="13"></line>
                            <line x1="16" y1="17" x2="8" y2="17"></line>
                        </svg>
                        <p>No MapReduce job output available yet</p>
                        <span class="empty-hint">Submit a job above to view actual word-count results here</span>
                    </div>
                </td>
            </tr>
        `;
        return;
    }

    const jid = resultsData.job_id || 'unknown';
    const uniqueCount = resultsData.unique_words || resultsData.results.length;
    const totalCount = resultsData.total_words || resultsData.results.reduce((sum, item) => sum + (item.count || 0), 0);
    const inSize = formatBytes(resultsData.input_size_bytes);
    const mapTasks = (resultsData.num_map_tasks !== undefined && resultsData.num_map_tasks !== null) ? resultsData.num_map_tasks : '—';
    const dur = formatDuration(resultsData.duration_seconds);
    const st = resultsData.status || 'COMPLETED';

    resultsJobIdBadge.textContent = `Job: ${jid}`;
    resultsJobIdBadge.className = 'pill-neutral font-mono text-cyan';
    const subText = resultsData.submission_time ? `Submitted: ${resultsData.submission_time} &bull; ` : '';
    resultsMetaTimestamp.innerHTML = `${subText}${uniqueCount} unique words`;

    resUniqueWords.textContent = uniqueCount;
    resTotalWords.textContent = totalCount;
    resInputSize.textContent = inSize;
    resMapTasks.textContent = mapTasks;
    resDuration.textContent = dur;

    resStatus.textContent = st;
    resStatus.className = st === 'COMPLETED' ? 'state-badge badge-completed' : 'state-badge badge-failed';

    // Filter results if query is present
    let displayList = resultsData.results;
    if (currentResultsFilter) {
        const q = currentResultsFilter.toLowerCase();
        displayList = displayList.filter(item => (item.word || '').toLowerCase().includes(q));
    }

    if (resultsFilterCount) {
        resultsFilterCount.textContent = `${displayList.length} / ${(resultsData.results || []).length} words`;
    }

    // Calculate total word count across all results in this job
    // Formula: relative_frequency = (word_count / total_words) * 100
    const allResults = resultsData.results || [];
    let totalWords = (resultsData.total_words !== undefined && resultsData.total_words !== null && Number(resultsData.total_words) > 0)
        ? Number(resultsData.total_words)
        : allResults.reduce((sum, item) => sum + (Number(item.count) || 0), 0);

    if (totalWords <= 0 && allResults.length > 0) {
        totalWords = allResults.reduce((sum, item) => sum + (Number(item.count) || 0), 0);
    }

    if (displayList.length === 0) {
        resultsTableBody.innerHTML = `
            <tr class="empty-row">
                <td colspan="4">
                    <div class="empty-state">
                        <p>${currentResultsFilter ? `No words matching "${currentResultsFilter}"` : 'No word counts available for this job'}</p>
                    </div>
                </td>
            </tr>
        `;
        return;
    }

    const rows = displayList.map((item, idx) => {
        const word = item.word || '';
        const count = Number(item.count) || 0;

        // Formula: relative_frequency = (word_count / total_words) * 100
        const relFreq = totalWords > 0 ? (count / totalWords) * 100 : 0;
        const pctDisplay = `${relFreq.toFixed(2)}%`;
        const barWidth = Math.min(100, Math.max(0, relFreq));

        return `
            <tr>
                <td><span class="font-mono text-muted text-xs">${idx + 1}</span></td>
                <td><span class="code-pill font-mono">${word}</span></td>
                <td><span class="font-mono" style="font-weight: 600; color: var(--text-primary);">${count}</span></td>
                <td>
                    <div class="freq-bar-container">
                        <div class="freq-bar-track" title="${pctDisplay}">
                            <div class="freq-bar-fill" style="width: ${barWidth}%;"></div>
                        </div>
                        <span class="freq-bar-pct">${pctDisplay}</span>
                    </div>
                </td>
            </tr>
        `;
    });

    resultsTableBody.innerHTML = rows.join('');
}

/**
 * Render Recent Job History Table
 */
function renderHistory(historyList) {
    if (!historyTableBody) return;
    const history = historyList || [];
    if (historyCountBadge) historyCountBadge.textContent = history.length;

    if (history.length === 0) {
        historyTableBody.innerHTML = `
            <tr class="empty-row" id="history-empty-row">
                <td colspan="7">
                    <div class="empty-state">
                        <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                            <circle cx="12" cy="12" r="10"></circle>
                            <polyline points="12 6 12 12 16 14"></polyline>
                        </svg>
                        <p>No past jobs recorded yet</p>
                        <span class="empty-hint">Completed and failed job runs will be catalogued here</span>
                    </div>
                </td>
            </tr>
        `;
        return;
    }

    const rows = history.map(job => {
        const jid = job.job_id || 'unknown';
        const subTime = job.submission_time || '--';
        const st = job.status || 'UNKNOWN';
        const tasks = job.num_map_tasks || 0;
        const unique = job.unique_words || 0;
        const dur = formatDuration(job.duration_seconds);

        let statusBadgeClass = 'badge-idle';
        if (st === 'COMPLETED') statusBadgeClass = 'badge-completed';
        else if (st === 'RUNNING') statusBadgeClass = 'badge-running';
        else if (st === 'FAILED') statusBadgeClass = 'badge-failed';

        const hasResults = st === 'COMPLETED';
        const actionBtn = hasResults
            ? `<button type="button" class="btn-action view-results-btn" data-job-id="${jid}">
                   <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                       <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"></path>
                       <circle cx="12" cy="12" r="3"></circle>
                   </svg>
                   <span>View Results</span>
               </button>`
            : '<span class="text-muted text-xs">—</span>';

        return `
            <tr>
                <td><span class="code-pill font-mono">${jid}</span></td>
                <td><span class="font-mono text-muted text-xs">${subTime}</span></td>
                <td><span class="state-badge ${statusBadgeClass}">${st}</span></td>
                <td><span class="font-mono">${tasks}</span></td>
                <td><span class="font-mono">${unique}</span></td>
                <td><span class="font-mono text-muted">${dur}</span></td>
                <td>${actionBtn}</td>
            </tr>
        `;
    });

    historyTableBody.innerHTML = rows.join('');

    // Attach event listeners to "View Results" buttons
    const viewBtns = historyTableBody.querySelectorAll('.view-results-btn');
    viewBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            const jid = btn.getAttribute('data-job-id');
            if (jid) {
                loadJobResults(jid);
            }
        });
    });
}

/**
 * FEATURE 1 — Render Distributed Cluster Topology
 */
function renderClusterMap(status, workers) {
    const clusterMasterStatusText = document.getElementById('cluster-master-status-text');
    const clusterWorkerCountPill = document.getElementById('cluster-worker-count-pill');
    const clusterTasksCompletedPill = document.getElementById('cluster-tasks-completed-pill');
    const masterBeaconDot = document.getElementById('master-beacon-dot');
    const masterStagePill = document.getElementById('master-stage-pill');
    const masterEndpointText = document.getElementById('master-endpoint-text');
    const masterActiveJobText = document.getElementById('master-active-job-text');
    const masterCompletedTasksText = document.getElementById('master-completed-tasks-text');
    const masterActiveWorkersText = document.getElementById('master-active-workers-text');
    const clusterWorkersGrid = document.getElementById('cluster-workers-grid');

    const isRunning = !!status.server_running;
    const workerCount = (workers || []).length;
    const completedTasks = status.total_completed_tasks || 0;
    const currentJobId = status.job_id || '—';
    const stage = (status.current_stage || 'IDLE').toUpperCase();

    // Summary pills
    if (clusterMasterStatusText) {
        clusterMasterStatusText.textContent = isRunning ? 'Master: Online' : 'Master: Offline';
    }
    if (clusterWorkerCountPill) {
        clusterWorkerCountPill.textContent = `${workerCount} Worker${workerCount === 1 ? '' : 's'} Connected`;
    }
    if (clusterTasksCompletedPill) {
        clusterTasksCompletedPill.textContent = `${completedTasks} Task${completedTasks === 1 ? '' : 's'} Finished`;
    }

    // Master node beacon and stage badge
    if (masterBeaconDot) {
        masterBeaconDot.className = isRunning ? 'master-beacon-dot online' : 'master-beacon-dot offline';
    }
    if (masterStagePill) {
        let badgeCls = 'badge-idle';
        if (!isRunning) badgeCls = 'badge-failed';
        else if (status.job_failed) badgeCls = 'badge-failed';
        else if (status.job_completed) badgeCls = 'badge-completed';
        else if (status.job_in_progress) badgeCls = 'badge-running';
        masterStagePill.className = `state-badge ${badgeCls}`;
        masterStagePill.textContent = stage;
    }

    // Master endpoint and metrics
    if (masterEndpointText) {
        const host = status.master_host || '127.0.0.1';
        const port = status.master_port || 5000;
        masterEndpointText.textContent = `TCP: ${host}:${port} • HTTP: 8080`;
    }
    if (masterActiveJobText) {
        masterActiveJobText.textContent = currentJobId;
    }
    if (masterCompletedTasksText) {
        masterCompletedTasksText.textContent = completedTasks;
    }
    if (masterActiveWorkersText) {
        masterActiveWorkersText.textContent = `${workerCount} online`;
    }

    // Render worker cards
    if (clusterWorkersGrid) {
        if (!workers || workers.length === 0) {
            clusterWorkersGrid.innerHTML = `
                <div class="cluster-empty-workers" id="cluster-empty-workers">
                    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                        <rect x="2" y="2" width="20" height="8" rx="2" ry="2"></rect>
                        <rect x="2" y="14" width="20" height="8" rx="2" ry="2"></rect>
                        <line x1="6" y1="6" x2="6.01" y2="6"></line>
                        <line x1="6" y1="18" x2="6.01" y2="18"></line>
                    </svg>
                    <p>No worker nodes currently registered</p>
                    <span class="empty-hint">Start a worker: <code>python workers/worker.py --worker-id worker-1</code></span>
                </div>
            `;
        } else {
            const cardsHtml = workers.map((w, idx) => {
                const wid = w.worker_id || `worker-${idx + 1}`;
                const st = (w.status || 'UNAVAILABLE').toUpperCase();
                const isBusy = st === 'BUSY';
                const isIdle = st === 'IDLE';
                const beaconCls = isBusy ? 'busy' : (isIdle ? 'idle' : 'unavailable');
                const cardCls = isBusy ? 'worker-busy' : (st === 'UNAVAILABLE' ? 'worker-unavailable' : '');
                const currentTask = (w.current_task && w.current_task !== '--' && w.current_task !== '—') ? w.current_task : 'Idle (Ready)';
                const compTasks = w.completed_tasks || 0;
                const hb = (w.seconds_since_heartbeat !== null && w.seconds_since_heartbeat !== undefined)
                    ? `${w.seconds_since_heartbeat.toFixed(1)}s ago`
                    : '--';
                const uptime = formatDuration(w.uptime_seconds);

                return `
                    <div class="cluster-worker-card ${cardCls}" id="worker-card-${wid}" data-worker-id="${wid}" data-status="${st}">
                        <div class="worker-connector-anchor" id="worker-anchor-${wid}"></div>
                        <div class="worker-card-header">
                            <div class="worker-id-wrapper">
                                <span class="worker-beacon ${beaconCls}"></span>
                                <span class="code-pill font-mono">${wid}</span>
                            </div>
                            <span class="badge-status ${isBusy ? 'status-busy' : (isIdle ? 'status-idle' : 'status-unavailable')}">
                                ${st}
                            </span>
                        </div>
                        <div class="worker-card-body">
                            <div class="worker-activity-row">
                                <span class="text-muted text-xs">Activity:</span>
                                <span class="font-mono text-xs ${isBusy ? 'text-cyan' : ''}" style="${isBusy ? 'font-weight: 600;' : ''}">${currentTask}</span>
                            </div>
                            <div class="worker-meta-grid">
                                <div><span>Completed:</span> <strong>${compTasks} tasks</strong></div>
                                <div><span>Heartbeat:</span> <strong>${hb}</strong></div>
                                <div><span>Uptime:</span> <strong>${uptime}</strong></div>
                                <div><span>Address:</span> <strong class="font-mono text-xs">${w.address || '--'}</strong></div>
                            </div>
                        </div>
                    </div>
                `;
            }).join('');

            clusterWorkersGrid.innerHTML = cardsHtml;
        }
    }

    // Draw SVG connection links
    requestAnimationFrame(drawClusterConnections);
}

/**
 * Draw dynamic SVG bezier curves between master anchor and worker nodes
 */
function drawClusterConnections() {
    const container = document.getElementById('cluster-topology-container');
    const svgLinksGroup = document.getElementById('cluster-svg-links');
    const masterAnchor = document.getElementById('master-anchor');
    if (!container || !svgLinksGroup || !masterAnchor) return;

    const workerCards = container.querySelectorAll('.cluster-worker-card');
    if (workerCards.length === 0) {
        svgLinksGroup.innerHTML = '';
        return;
    }

    const containerRect = container.getBoundingClientRect();
    const masterRect = masterAnchor.getBoundingClientRect();
    const x1 = masterRect.left + masterRect.width / 2 - containerRect.left;
    const y1 = masterRect.top + masterRect.height / 2 - containerRect.top;

    let paths = '';
    workerCards.forEach(card => {
        const wid = card.getAttribute('data-worker-id');
        const st = card.getAttribute('data-status') || 'IDLE';
        const anchor = card.querySelector('.worker-connector-anchor') || card;
        const anchorRect = anchor.getBoundingClientRect();
        const x2 = anchorRect.left + anchorRect.width / 2 - containerRect.left;
        const y2 = anchorRect.top + anchorRect.height / 2 - containerRect.top;

        const cy1 = y1 + (y2 - y1) * 0.5;
        const cy2 = y1 + (y2 - y1) * 0.5;
        const d = `M ${x1} ${y1} C ${x1} ${cy1}, ${x2} ${cy2}, ${x2} ${y2}`;

        let linkClass = 'link-idle';
        if (st === 'BUSY') linkClass = 'link-busy';
        else if (st === 'UNAVAILABLE') linkClass = 'link-unavailable';

        paths += `<path d="${d}" class="svg-cluster-link ${linkClass}" data-worker-id="${wid}" />`;
    });

    svgLinksGroup.innerHTML = paths;
}

/**
 * FEATURE 2 — Render Live Task Distribution Grid
 */
function renderTaskDistribution(tasks) {
    const board = document.getElementById('task-distribution-grid');
    if (!board) return;

    if (!tasks || tasks.length === 0) {
        board.innerHTML = `
            <div class="empty-state task-board-empty" id="task-board-empty">
                <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                    <rect x="3" y="3" width="7" height="7"></rect>
                    <rect x="14" y="3" width="7" height="7"></rect>
                    <rect x="14" y="14" width="7" height="7"></rect>
                    <rect x="3" y="14" width="7" height="7"></rect>
                </svg>
                <p>No map tasks created yet</p>
                <span class="empty-hint">Submit a job above to view live task assignments across workers</span>
            </div>
        `;
        return;
    }

    const cardsHtml = tasks.map(t => {
        const tid = t.task_id || 'task';
        const st = (t.state || 'pending').toLowerCase();
        const retries = t.retries || 0;
        const maxRetries = t.max_retries || 3;
        const worker = t.assigned_worker || null;
        const reason = (t.failure_reason && t.failure_reason !== '--' && t.failure_reason !== '—') ? t.failure_reason : null;

        let cardClass = '';
        let badgeClass = 'badge-idle';
        let badgeText = st.toUpperCase();

        if (st === 'assigned') {
            cardClass = 'card-assigned';
            badgeClass = 'badge-running';
        } else if (st === 'completed') {
            cardClass = 'card-completed';
            badgeClass = 'badge-completed';
        } else if (st === 'failed') {
            cardClass = 'card-failed';
            badgeClass = 'badge-failed';
        }

        const workerChip = worker
            ? `<span class="code-pill text-cyan font-mono">${worker}</span>`
            : `<span class="text-muted text-xs">Unassigned (Queue)</span>`;

        const failureHtml = reason
            ? `<div class="text-rose text-xs" style="margin-top: 0.2rem; word-break: break-word;">${reason}</div>`
            : '';

        return `
            <div class="task-distribution-card ${cardClass}" data-task-id="${tid}">
                <div class="task-card-top">
                    <span class="code-pill font-mono">${tid}</span>
                    <span class="state-badge ${badgeClass}" style="font-size: 0.7rem; padding: 0.15rem 0.5rem;">${badgeText}</span>
                </div>
                <div class="task-card-worker-row">
                    <span class="text-muted text-xs">Worker:</span>
                    ${workerChip}
                </div>
                ${failureHtml}
                <div class="task-card-footer">
                    <span>Retries:</span>
                    <span class="task-retry-pill font-mono">${retries} / ${maxRetries}</span>
                </div>
            </div>
        `;
    }).join('');

    board.innerHTML = cardsHtml;
}

/**
 * FEATURE 3 — Interactive Execution Timeline
 */
function renderExecutionTimeline(status, latestResults, workers) {
    const stage = (status.current_stage || 'IDLE').toUpperCase();
    const stats = status.stats || {};
    const totalTasks = stats.total || 0;
    const completedTasks = stats.completed || 0;
    const workerCount = (workers || []).length;
    const inProgress = !!status.job_in_progress;
    const isCompleted = !!status.job_completed || stage === 'COMPLETED';
    const isFailed = !!status.job_failed || stage === 'FAILED';

    const ts = status.phase_timestamps || (latestResults && latestResults.phase_timestamps) || {};

    const stepReady = document.getElementById('step-registered');
    const stepSubmitted = document.getElementById('step-submitted');
    const stepMap = document.getElementById('step-map');
    const stepShuffle = document.getElementById('step-shuffle');
    const stepReduce = document.getElementById('step-reduce');
    const stepDone = document.getElementById('step-done');

    const div1 = document.getElementById('divider-ready-submitted');
    const div2 = document.getElementById('divider-submitted-map');
    const div3 = document.getElementById('divider-map-shuffle');
    const div4 = document.getElementById('divider-shuffle-reduce');
    const div5 = document.getElementById('divider-reduce-done');

    // Reset classes
    [stepReady, stepSubmitted, stepMap, stepShuffle, stepReduce, stepDone].forEach(s => {
        if (s) s.className = 'step-item';
    });
    [div1, div2, div3, div4, div5].forEach(d => {
        if (d) d.className = 'step-divider';
    });

    // 1. Ready
    if (workerCount > 0) {
        if (stepReady) stepReady.classList.add('completed');
        const meta = document.getElementById('step-meta-ready');
        if (meta) meta.textContent = `${workerCount} worker${workerCount > 1 ? 's' : ''} ready`;
    } else {
        const meta = document.getElementById('step-meta-ready');
        if (meta) meta.textContent = 'Waiting for workers';
    }

    // 2. Submitted
    if (inProgress || isCompleted || isFailed) {
        if (div1) div1.classList.add('completed');
        if (stepSubmitted) stepSubmitted.classList.add('completed');
        const meta = document.getElementById('step-meta-submitted');
        if (meta) meta.textContent = ts.submitted ? formatTimestamp(ts.submitted) : 'Accepted';
    } else {
        const meta = document.getElementById('step-meta-submitted');
        if (meta) meta.textContent = 'Awaiting input';
    }

    // 3. Map Phase
    if (inProgress) {
        if (div2) div2.classList.add('active');
        if (stage === 'SHUFFLE_AND_REDUCE' || (completedTasks === totalTasks && totalTasks > 0)) {
            if (stepMap) stepMap.classList.add('completed');
            if (div2) div2.classList.replace('active', 'completed');
            const meta = document.getElementById('step-meta-map');
            if (meta) meta.textContent = `${completedTasks}/${totalTasks} mapped`;
        } else {
            if (stepMap) stepMap.classList.add('active');
            const meta = document.getElementById('step-meta-map');
            if (meta) meta.textContent = `Running (${completedTasks}/${totalTasks})`;
        }
    } else if (isCompleted) {
        if (div2) div2.classList.add('completed');
        if (stepMap) stepMap.classList.add('completed');
        const meta = document.getElementById('step-meta-map');
        if (meta) meta.textContent = 'All tasks mapped';
    } else if (isFailed) {
        if (stepMap) stepMap.classList.add('failed');
        const meta = document.getElementById('step-meta-map');
        if (meta) meta.textContent = 'Mapping failed';
    } else {
        const meta = document.getElementById('step-meta-map');
        if (meta) meta.textContent = 'Pending';
    }

    // 4. Shuffle & Group
    if (inProgress && (stage === 'SHUFFLE_AND_REDUCE' || (completedTasks === totalTasks && totalTasks > 0))) {
        if (div3) div3.classList.add('active');
        if (stepShuffle) stepShuffle.classList.add('active');
        const meta = document.getElementById('step-meta-shuffle');
        if (meta) meta.textContent = 'Grouping intermediate';
    } else if (isCompleted) {
        if (div3) div3.classList.add('completed');
        if (stepShuffle) stepShuffle.classList.add('completed');
        const meta = document.getElementById('step-meta-shuffle');
        if (meta) meta.textContent = 'Keys grouped';
    } else {
        const meta = document.getElementById('step-meta-shuffle');
        if (meta) meta.textContent = 'Pending';
    }

    // 5. Reduce Phase
    if (inProgress && stage === 'SHUFFLE_AND_REDUCE') {
        if (div4) div4.classList.add('active');
        if (stepReduce) stepReduce.classList.add('active');
        const meta = document.getElementById('step-meta-reduce');
        if (meta) meta.textContent = 'Computing counts';
    } else if (isCompleted) {
        if (div4) div4.classList.add('completed');
        if (stepReduce) stepReduce.classList.add('completed');
        const meta = document.getElementById('step-meta-reduce');
        if (meta) meta.textContent = 'Frequencies done';
    } else {
        const meta = document.getElementById('step-meta-reduce');
        if (meta) meta.textContent = 'Pending';
    }

    // 6. Complete
    if (isCompleted) {
        if (div5) div5.classList.add('completed');
        if (stepDone) stepDone.classList.add('completed');
        const meta = document.getElementById('step-meta-done');
        if (meta) meta.textContent = status.duration_seconds !== null ? `${formatDuration(status.duration_seconds)}` : 'Finished';
    } else if (isFailed) {
        if (stepDone) stepDone.classList.add('failed');
        const meta = document.getElementById('step-meta-done');
        if (meta) meta.textContent = 'Job Failed';
    } else {
        const meta = document.getElementById('step-meta-done');
        if (meta) meta.textContent = 'Pending';
    }

    // Highlight the selected phase element
    document.querySelectorAll('.step-item[data-phase]').forEach(item => {
        if (item.getAttribute('data-phase') === selectedTimelinePhase) {
            item.style.filter = 'drop-shadow(0 0 6px rgba(99, 102, 241, 0.7))';
        } else {
            item.style.filter = 'none';
        }
    });

    updateTimelineInspector(status, latestResults, workers);
}

function formatTimestamp(epochSec) {
    if (!epochSec || isNaN(epochSec)) return '--';
    const d = new Date(epochSec * 1000);
    return d.toLocaleTimeString();
}

function updateTimelineInspector(status, latestResults, workers) {
    const inspectorBadge = document.getElementById('inspector-badge');
    const inspectorTitle = document.getElementById('inspector-title');
    const inspectorStatusPill = document.getElementById('inspector-status-pill');
    const inspectorDesc = document.getElementById('inspector-desc');
    const inspectorStartTime = document.getElementById('inspector-start-time');
    const inspectorEndTime = document.getElementById('inspector-end-time');
    const inspectorDuration = document.getElementById('inspector-duration');
    const inspectorDetails = document.getElementById('inspector-details');

    if (!inspectorTitle) return;

    const phaseObj = TIMELINE_PHASES.find(p => p.id === selectedTimelinePhase) || TIMELINE_PHASES[0];
    const ts = status.phase_timestamps || (latestResults && latestResults.phase_timestamps) || {};
    const stage = (status.current_stage || 'IDLE').toUpperCase();
    const stats = status.stats || {};

    inspectorBadge.textContent = `Phase: ${phaseObj.id.toUpperCase()}`;
    inspectorTitle.textContent = phaseObj.title;
    inspectorDesc.textContent = phaseObj.desc;

    let pStatus = 'Pending';
    let pStart = '--';
    let pEnd = '--';
    let pDur = '--';
    let pDetails = '--';

    if (phaseObj.id === 'ready') {
        const wc = (workers || []).length;
        pStatus = wc > 0 ? 'Cluster Ready' : 'Awaiting Workers';
        pStart = ts.ready ? formatTimestamp(ts.ready) : 'Master Online';
        pDetails = `${wc} worker node(s) registered on port 5000`;
    } else if (phaseObj.id === 'submitted') {
        if (status.job_id) {
            pStatus = 'Submitted & Accepted';
            pStart = ts.submitted ? formatTimestamp(ts.submitted) : '--';
            pDetails = `Job ${status.job_id} (${stats.total || 0} tasks created)`;
        }
    } else if (phaseObj.id === 'map') {
        if (status.job_in_progress && stage === 'MAPPING') {
            pStatus = 'In Progress (Active)';
            pStart = ts.map_start ? formatTimestamp(ts.map_start) : '--';
            pDetails = `Executing ${stats.assigned || 0} in-flight, ${stats.completed || 0} completed`;
        } else if (ts.map_end || status.job_completed) {
            pStatus = 'Completed';
            pStart = ts.map_start ? formatTimestamp(ts.map_start) : '--';
            pEnd = ts.map_end ? formatTimestamp(ts.map_end) : '--';
            if (ts.map_start && ts.map_end) {
                pDur = `${(ts.map_end - ts.map_start).toFixed(2)}s`;
            }
            pDetails = `All ${stats.total || 0} map tasks processed`;
        }
    } else if (phaseObj.id === 'shuffle') {
        if (status.job_in_progress && stage === 'SHUFFLE_AND_REDUCE') {
            pStatus = 'In Progress (Active)';
            pStart = ts.shuffle_start ? formatTimestamp(ts.shuffle_start) : '--';
            pDetails = 'Aggregating intermediate key-value pairs';
        } else if (ts.shuffle_end || status.job_completed) {
            pStatus = 'Completed';
            pStart = ts.shuffle_start ? formatTimestamp(ts.shuffle_start) : '--';
            pEnd = ts.shuffle_end ? formatTimestamp(ts.shuffle_end) : '--';
            if (ts.shuffle_start && ts.shuffle_end) {
                pDur = `${(ts.shuffle_end - ts.shuffle_start).toFixed(2)}s`;
            }
            pDetails = 'Intermediate pairs grouped by word';
        }
    } else if (phaseObj.id === 'reduce') {
        if (status.job_in_progress && stage === 'SHUFFLE_AND_REDUCE') {
            pStatus = 'In Progress (Active)';
            pStart = ts.reduce_start ? formatTimestamp(ts.reduce_start) : '--';
            pDetails = 'Computing word counts & frequencies';
        } else if (ts.reduce_end || status.job_completed) {
            pStatus = 'Completed';
            pStart = ts.reduce_start ? formatTimestamp(ts.reduce_start) : '--';
            pEnd = ts.reduce_end ? formatTimestamp(ts.reduce_end) : '--';
            if (ts.reduce_start && ts.reduce_end) {
                pDur = `${(ts.reduce_end - ts.reduce_start).toFixed(2)}s`;
            }
            pDetails = 'Frequencies computed & output generated';
        }
    } else if (phaseObj.id === 'complete') {
        if (status.job_completed) {
            pStatus = 'Completed';
            pStart = ts.submitted ? formatTimestamp(ts.submitted) : '--';
            pEnd = ts.complete ? formatTimestamp(ts.complete) : '--';
            pDur = formatDuration(status.duration_seconds);
            pDetails = 'Results ready for download / inspection';
        } else if (status.job_failed) {
            pStatus = 'Failed';
            pDetails = status.job_error || 'Execution failed';
        }
    }

    inspectorStatusPill.textContent = pStatus;
    inspectorStartTime.textContent = pStart;
    inspectorEndTime.textContent = pEnd;
    inspectorDuration.textContent = pDur;
    inspectorDetails.textContent = pDetails;
}

/**
 * FEATURE 4 — Render Job Performance & Comparison
 */
function renderJobComparison(history) {
    const kpiTotalJobs = document.getElementById('kpi-total-jobs');
    const kpiAvgDuration = document.getElementById('kpi-avg-duration');
    const kpiTotalWords = document.getElementById('kpi-total-words');
    const kpiFastestDuration = document.getElementById('kpi-fastest-duration');
    const tbody = document.getElementById('job-comparison-tbody');

    if (!tbody) return;
    const list = history || [];

    if (list.length === 0) {
        if (kpiTotalJobs) kpiTotalJobs.textContent = '0';
        if (kpiAvgDuration) kpiAvgDuration.textContent = '--';
        if (kpiTotalWords) kpiTotalWords.textContent = '0';
        if (kpiFastestDuration) kpiFastestDuration.textContent = '--';
        tbody.innerHTML = `
            <tr class="empty-row" id="comparison-empty-row">
                <td colspan="8">
                    <div class="empty-state">
                        <p>No job comparison data available yet</p>
                        <span class="empty-hint">Submit jobs with different inputs to compare cluster execution metrics</span>
                    </div>
                </td>
            </tr>
        `;
        return;
    }

    // Calculate real aggregate KPIs
    const validDurs = list.filter(j => j.duration_seconds && !isNaN(j.duration_seconds)).map(j => Number(j.duration_seconds));
    const avgDur = validDurs.length > 0 ? (validDurs.reduce((a, b) => a + b, 0) / validDurs.length) : null;
    const fastestDur = validDurs.length > 0 ? Math.min(...validDurs) : null;
    const sumWords = list.reduce((sum, j) => sum + (Number(j.total_words) || 0), 0);

    if (kpiTotalJobs) kpiTotalJobs.textContent = list.length;
    if (kpiAvgDuration) kpiAvgDuration.textContent = avgDur !== null ? formatDuration(avgDur) : '--';
    if (kpiTotalWords) kpiTotalWords.textContent = sumWords;
    if (kpiFastestDuration) kpiFastestDuration.textContent = fastestDur !== null ? formatDuration(fastestDur) : '--';

    // Populate comparison rows
    const rowsHtml = list.map(j => {
        const jid = j.job_id || 'unknown';
        const st = j.status || 'UNKNOWN';
        const tasks = j.num_map_tasks || 0;
        const inSize = formatBytes(j.input_size_bytes);
        const totalW = j.total_words || 0;
        const uniqueW = j.unique_words || 0;
        const dur = formatDuration(j.duration_seconds);

        let badgeCls = 'badge-idle';
        if (st === 'COMPLETED') badgeCls = 'badge-completed';
        else if (st === 'RUNNING') badgeCls = 'badge-running';
        else if (st === 'FAILED') badgeCls = 'badge-failed';

        const actionBtn = st === 'COMPLETED'
            ? `<button type="button" class="btn-action view-results-btn" data-job-id="${jid}">
                   <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                       <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"></path>
                       <circle cx="12" cy="12" r="3"></circle>
                   </svg>
                   <span>View</span>
               </button>`
            : '<span class="text-muted text-xs">—</span>';

        return `
            <tr>
                <td><span class="code-pill font-mono">${jid}</span></td>
                <td><span class="state-badge ${badgeCls}">${st}</span></td>
                <td><span class="font-mono">${tasks}</span></td>
                <td><span class="font-mono text-muted">${inSize}</span></td>
                <td><span class="font-mono">${totalW}</span></td>
                <td><span class="font-mono text-cyan">${uniqueW}</span></td>
                <td><span class="font-mono">${dur}</span></td>
                <td>${actionBtn}</td>
            </tr>
        `;
    }).join('');

    tbody.innerHTML = rowsHtml;

    // Attach click handlers to comparison view buttons
    const btns = tbody.querySelectorAll('.view-results-btn');
    btns.forEach(btn => {
        btn.addEventListener('click', () => {
            const jid = btn.getAttribute('data-job-id');
            if (jid) loadJobResults(jid);
        });
    });
}

/**
 * Load results for a specific job into the Results panel
 */
async function loadJobResults(jobId) {
    try {
        const res = await fetch(`/api/results/${encodeURIComponent(jobId)}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (data.available) {
            renderResults(data);
            if (resultsSection) {
                resultsSection.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
            }
        } else {
            showFeedback('error', `Results for job ${jobId} are not available.`);
        }
    } catch (err) {
        console.warn('Failed to fetch job results:', err);
        showFeedback('error', `Failed to load results for ${jobId}: ${err.message}`);
    }
}

/**
 * Display inline feedback message in the submit job card
 */
function showFeedback(type, message) {
    if (!submitFeedback || !submitFeedbackMsg) return;
    submitFeedback.className = `feedback-banner ${type}`;
    submitFeedbackMsg.textContent = message;
    if (feedbackIcon) {
        feedbackIcon.textContent = type === 'success' ? '✓' : '⚠';
    }
    submitFeedback.classList.remove('hidden');

    // Auto-hide success messages after 6 seconds
    if (type === 'success') {
        setTimeout(() => {
            if (submitFeedback) submitFeedback.classList.add('hidden');
        }, 6000);
    }
}

/**
 * Fetch all data from dashboard API
 */
async function fetchDashboardData() {
    try {
        const summaryRes = await fetch('/api/summary');
        if (!summaryRes.ok) {
            throw new Error(`Summary API error: HTTP ${summaryRes.status}`);
        }
        const summary = await summaryRes.json();

        const status = summary.status || {};
        const workers = summary.workers || [];
        const tasks = summary.tasks || [];
        const history = summary.job_history || [];
        const latestResults = summary.latest_results || null;

        if (status.server_running) {
            setConnectionState(true);
        } else {
            setConnectionState(false, 'Master server is not currently running');
        }

        currentSummaryData = summary;

        updateJobStatus(status, workers, tasks);
        renderClusterMap(status, workers);
        renderExecutionTimeline(status, latestResults, workers);
        renderTaskDistribution(tasks);
        renderWorkers(workers);
        renderTasks(tasks);
        renderHistory(history);
        renderJobComparison(history);

        // Update results panel if a new job completed or if latest results have finalized/updated
        if (latestResults) {
            const isDifferentJob = !currentResultsData || currentResultsData.job_id !== latestResults.job_id;
            const isNewlyFinalized = currentResultsData && currentResultsData.job_id === latestResults.job_id && (
                currentResultsData.status !== latestResults.status ||
                (!currentResultsData.duration_seconds && latestResults.duration_seconds) ||
                (!currentResultsData.input_size_bytes && latestResults.input_size_bytes)
            );
            if (isDifferentJob || isNewlyFinalized) {
                renderResults(latestResults);
            }
        }

        const now = new Date();
        lastUpdatedEl.textContent = `Updated: ${now.toLocaleTimeString()}`;
    } catch (err) {
        console.warn('Dashboard fetch error:', err.message);
        setConnectionState(false, `Cannot reach master API: ${err.message}. Retrying...`);
    }
}

/**
 * Handle Job Submission Form Submit
 */
async function handleJobSubmit() {
    if (isSubmitting) return;

    // Determine active input mode
    const isTextMode = modeTabText.classList.contains('active');
    const mappersCount = parseInt(jobNumMappers.value, 10) || 2;

    if (mappersCount < 1 || mappersCount > 32) {
        showFeedback('error', 'Please configure between 1 and 32 map tasks.');
        return;
    }

    let payload;
    let isFormData = false;

    if (isTextMode) {
        const textVal = (jobInputText.value || '').trim();
        if (!textVal) {
            showFeedback('error', 'Please enter text to submit, or switch to file upload.');
            jobInputText.focus();
            return;
        }
        payload = JSON.stringify({
            input_text: textVal,
            num_mappers: mappersCount
        });
    } else {
        const file = jobFileInput.files && jobFileInput.files[0];
        if (!file) {
            showFeedback('error', 'Please select a .txt file to upload.');
            return;
        }
        if (!file.name.toLowerCase().endsWith('.txt')) {
            showFeedback('error', 'Only .txt files are accepted.');
            return;
        }
        const formData = new FormData();
        formData.append('file', file);
        formData.append('num_mappers', mappersCount.toString());
        payload = formData;
        isFormData = true;
    }

    // Set UI loading state
    isSubmitting = true;
    submitJobBtn.disabled = true;
    submitBtnText.textContent = 'Submitting Job...';
    submitSpinner.classList.remove('hidden');
    submitIcon.classList.add('hidden');
    submitFeedback.classList.add('hidden');

    try {
        const response = await fetch('/api/submit-job', {
            method: 'POST',
            headers: isFormData ? {} : { 'Content-Type': 'application/json' },
            body: payload
        });

        const data = await response.json();

        if (response.ok && data.success) {
            showFeedback('success', `Job ${data.job_id} submitted successfully with ${data.num_map_tasks} map tasks! Processing across workers...`);
            // Trigger rapid refresh to immediately show task states
            fetchDashboardData();
            setTimeout(fetchDashboardData, 600);
        } else {
            const errMsg = data.error || `Submission failed with status ${response.status}`;
            showFeedback('error', errMsg);
        }
    } catch (err) {
        showFeedback('error', `Network error submitting job: ${err.message}`);
    } finally {
        isSubmitting = false;
        submitSpinner.classList.add('hidden');
        submitIcon.classList.remove('hidden');
        submitBtnText.textContent = 'Run MapReduce Job';
        submitJobBtn.disabled = false;
    }
}

/**
 * Sample sentences for quick demonstration
 */
const SAMPLE_DEMO_TEXT = `Distributed systems process large-scale datasets by orchestrating independent tasks across cluster workers.
The master coordinates task assignment, fault tolerance, and worker heartbeats over TCP port 5000.
Map tasks partition input lines into key-value pairs of words and occurrences.
Shuffle and group aggregates intermediate map results by word across the distributed network.
Reducers compute the final word count frequencies and generate the sorted output file.
When workers fail or disconnect unexpectedly, the scheduler automatically requeues uncompleted tasks for fault tolerance.`;

/**
 * Setup event listeners for filtering, forms, and buttons
 */
function setupEventListeners() {
    // Manual refresh button
    refreshBtn.addEventListener('click', () => {
        refreshBtn.classList.add('spinning');
        fetchDashboardData().finally(() => {
            setTimeout(() => refreshBtn.classList.remove('spinning'), 300);
        });
    });

    // Task state filter buttons
    const filterBtns = document.querySelectorAll('.filter-btn');
    filterBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            filterBtns.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            currentFilter = btn.getAttribute('data-filter') || 'all';
            renderTasks(lastTasksData);
        });
    });

    // Mode tabs (Text vs File)
    modeTabText.addEventListener('click', () => {
        modeTabText.classList.add('active');
        modeTabFile.classList.remove('active');
        textInputContainer.classList.remove('hidden');
        fileInputContainer.classList.add('hidden');
    });

    modeTabFile.addEventListener('click', () => {
        modeTabFile.classList.add('active');
        modeTabText.classList.remove('active');
        fileInputContainer.classList.remove('hidden');
        textInputContainer.classList.add('hidden');
    });

    // Text area character / word count stats
    jobInputText.addEventListener('input', () => {
        const text = jobInputText.value || '';
        const charCount = text.length;
        const wordCount = text.trim() ? text.trim().split(/\s+/).length : 0;
        textInputStats.textContent = `${charCount} characters • ${wordCount} words`;
    });

    // File input change & drop handlers
    jobFileInput.addEventListener('change', () => {
        const file = jobFileInput.files && jobFileInput.files[0];
        if (file) {
            selectedFileName.textContent = `${file.name} (${formatBytes(file.size)})`;
            selectedFileBadge.classList.remove('hidden');
        } else {
            selectedFileBadge.classList.add('hidden');
        }
    });

    removeFileBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        jobFileInput.value = '';
        selectedFileBadge.classList.add('hidden');
    });

    // Drag and drop events for file area
    ['dragenter', 'dragover'].forEach(eventName => {
        fileDropArea.addEventListener(eventName, (e) => {
            e.preventDefault();
            fileDropArea.classList.add('dragover');
        });
    });
    ['dragleave', 'drop'].forEach(eventName => {
        fileDropArea.addEventListener(eventName, (e) => {
            e.preventDefault();
            fileDropArea.classList.remove('dragover');
        });
    });
    fileDropArea.addEventListener('drop', (e) => {
        if (e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files.length > 0) {
            jobFileInput.files = e.dataTransfer.files;
            const file = e.dataTransfer.files[0];
            selectedFileName.textContent = `${file.name} (${formatBytes(file.size)})`;
            selectedFileBadge.classList.remove('hidden');
        }
    });

    // Map tasks preset pills
    presetPills.forEach(pill => {
        pill.addEventListener('click', () => {
            presetPills.forEach(p => p.classList.remove('active'));
            pill.classList.add('active');
            const val = pill.getAttribute('data-val');
            jobNumMappers.value = val;
            if (numMappersVal) numMappersVal.textContent = `${val} tasks`;
        });
    });

    jobNumMappers.addEventListener('input', () => {
        const val = jobNumMappers.value;
        if (numMappersVal) numMappersVal.textContent = `${val || 1} tasks`;
        presetPills.forEach(p => {
            if (p.getAttribute('data-val') === val) p.classList.add('active');
            else p.classList.remove('active');
        });
    });

    // Load sample text
    loadSampleBtn.addEventListener('click', () => {
        modeTabText.click();
        jobInputText.value = SAMPLE_DEMO_TEXT;
        jobInputText.dispatchEvent(new Event('input'));
        showFeedback('success', 'Demo sample text loaded! Click "Run MapReduce Job" to execute.');
    });

    // Clear form
    clearFormBtn.addEventListener('click', () => {
        jobInputText.value = '';
        jobInputText.dispatchEvent(new Event('input'));
        jobFileInput.value = '';
        selectedFileBadge.classList.add('hidden');
        submitFeedback.classList.add('hidden');
        jobNumMappers.value = '2';
        jobNumMappers.dispatchEvent(new Event('input'));
    });

    // Form submit
    jobSubmitForm.addEventListener('submit', (e) => {
        e.preventDefault();
        handleJobSubmit();
    });
    submitJobBtn.addEventListener('click', (e) => {
        e.preventDefault();
        handleJobSubmit();
    });

    // Results search input
    if (resultsSearchInput) {
        resultsSearchInput.addEventListener('input', () => {
            currentResultsFilter = (resultsSearchInput.value || '').trim();
            if (currentResultsData) {
                renderResults(currentResultsData);
            }
        });
    }

    // Timeline step click listeners for phase inspection
    document.querySelectorAll('.step-item[data-phase]').forEach(item => {
        item.addEventListener('click', () => {
            selectedTimelinePhase = item.getAttribute('data-phase');
            if (currentSummaryData) {
                renderExecutionTimeline(
                    currentSummaryData.status || {},
                    currentSummaryData.latest_results || null,
                    currentSummaryData.workers || []
                );
            }
        });
    });

    // Window resize handler to reposition SVG cluster topology paths
    window.addEventListener('resize', drawClusterConnections);
}

// Initialization on DOMContentLoaded
document.addEventListener('DOMContentLoaded', () => {
    setupEventListeners();
    fetchDashboardData();
    pollTimer = setInterval(fetchDashboardData, POLL_INTERVAL_MS);
});
