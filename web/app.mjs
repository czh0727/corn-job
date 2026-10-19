const API = '/api/v1'

export async function requestJSON(path, options = {}, fetcher = fetch) {
  const response = await fetcher(`${API}${path}`, {
    ...options,
    headers: options.body ? { 'Content-Type': 'application/json' } : undefined,
    body: options.body ? JSON.stringify(options.body) : undefined,
  })
  if (!response.ok) {
    let message = `请求失败（${response.status}）`
    try {
      const error = await response.json()
      if (error.error) message = error.error
    } catch { /* HTTP 错误可能没有 JSON 响应体 */ }
    throw new Error(message)
  }
  return response.status === 204 ? null : response.json()
}

export function taskFormInput(values, now = Date.now()) {
  const name = values.name.trim()
  if (!name) throw new Error('请输入任务名称')
  const scheduleType = values.schedule_type
  if (scheduleType !== 'CRON' && scheduleType !== 'DELAYED') throw new Error('请选择调度类型')
  let payload
  try {
    payload = JSON.parse(values.payload)
  } catch {
    throw new Error('Payload 不是合法 JSON')
  }
  if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) {
    throw new Error('Payload 必须是 JSON 对象')
  }
  const concurrency = Number(values.concurrency)
  const maxRetry = Number(values.max_retry)
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 10) throw new Error('Worker 并发需为 1–10')
  if (!Number.isInteger(maxRetry) || maxRetry < 0 || maxRetry > 20) throw new Error('失败重试需为 0–20')

  const definition = {
    name,
    description: values.description.trim(),
    schedule_type: scheduleType,
    payload,
  }
  if (scheduleType === 'CRON') {
    const expression = values.cron_expression.trim()
    const timezone = values.timezone.trim()
    if (!expression) throw new Error('请输入 Cron 表达式')
    if (!timezone) throw new Error('请输入时区')
    definition.cron_expression = expression
    definition.timezone = timezone
  } else {
    const executeAt = new Date(values.execute_at).getTime()
    if (!Number.isFinite(executeAt) || executeAt <= now) throw new Error('执行时间必须晚于当前时间')
    definition.execute_at_ms = executeAt
  }
  return { definition, worker: { handler_type: 'log', concurrency, max_retry: maxRetry } }
}

function escapeHTML(value) {
  return String(value ?? '').replace(/[&<>"']/g, (character) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[character])
}

const $ = (id) => document.getElementById(id)
const state = { tasks: [], executions: [], page: 1, total: 0, tab: 'tasks', editing: null }
const statusLabel = { DRAFT: '草稿', ACTIVE: '运行中', PAUSED: '已暂停', FINISHED: '已结束', RUNNING: '运行中', STOPPED: '已停止', RETRYING: '待重试', SUCCEEDED: '成功', FAILED: '失败', CANCELED: '已取消' }
let toastTimer

function formatTime(ms) {
  return ms ? new Date(ms).toLocaleString('zh-CN', { hour12: false }) : '—'
}

function toast(message, error = false) {
  const box = $('toast')
  box.textContent = message
  box.classList.toggle('error', error)
  box.hidden = false
  clearTimeout(toastTimer)
  toastTimer = setTimeout(() => { box.hidden = true }, 4500)
}

function badge(status) {
  return `<span class="badge badge-${escapeHTML(status.toLowerCase())}"><span class="badge-dot"></span>${escapeHTML(statusLabel[status] || status)}</span>`
}

function taskName(id) {
  return state.tasks.find((task) => task.id === id)?.name || `#${id}`
}

function switchTab(tab) {
  state.tab = tab
  $('tasks-panel').hidden = tab !== 'tasks'
  $('executions-panel').hidden = tab !== 'executions'
  document.querySelectorAll('.tab').forEach((button) => {
    const active = button.dataset.tab === tab
    button.classList.toggle('active', active)
    button.setAttribute('aria-selected', String(active))
  })
  if (tab === 'executions') void loadExecutions()
}

function renderTasks() {
  const tasks = state.tasks
  $('stat-total').textContent = tasks.length
  $('stat-active').textContent = tasks.filter((task) => task.status === 'ACTIVE').length
  $('stat-paused').textContent = tasks.filter((task) => task.status === 'PAUSED').length
  $('stat-workers').textContent = tasks.filter((task) => task.worker.status === 'RUNNING').length
  $('task-count').textContent = `${tasks.length} 个任务`
  $('tasks-table').hidden = tasks.length === 0
  $('tasks-empty').hidden = tasks.length > 0
  $('task-rows').innerHTML = tasks.map((task) => {
    const schedule = task.schedule_type === 'CRON'
      ? `<strong class="mono">${escapeHTML(task.cron_expression)}</strong><small>${escapeHTML(task.timezone)}</small>`
      : `<strong>单次执行</strong><small>${escapeHTML(formatTime(task.execute_at_ms))}</small>`
    const stateAction = task.status === 'DRAFT' ? `<button data-action="publish">发布</button>`
      : task.status === 'ACTIVE' ? `<button data-action="pause">暂停</button>`
        : task.status === 'PAUSED' ? `<button data-action="resume">恢复</button>` : ''
    const editAction = ['DRAFT', 'PAUSED'].includes(task.status) ? `<button data-action="edit">编辑</button>` : ''
    const runAction = ['ACTIVE', 'PAUSED'].includes(task.status) ? `<button data-action="run">立即执行</button>` : ''
    const workerAction = task.worker.status === 'RUNNING' ? `<button data-action="stop-worker">停 Worker</button>` : `<button data-action="start-worker">启 Worker</button>`
    return `<tr data-id="${task.id}"><td><button class="task-link" data-action="detail">${escapeHTML(task.name)}</button><small class="task-id">#${task.id} · ${escapeHTML(task.description || '无说明')}</small></td><td>${schedule}</td><td>${badge(task.status)}</td><td>${badge(task.worker.status)}<small>${escapeHTML(task.worker.handler_type)} · 并发 ${task.worker.concurrency}</small></td><td class="muted">${escapeHTML(formatTime(task.updated_at_ms))}</td><td><div class="row-actions">${stateAction}${runAction}${editAction}${workerAction}<button data-action="logs">记录</button></div></td></tr>`
  }).join('')

  const filter = $('execution-task')
  const previous = filter.value
  filter.innerHTML = '<option value="">全部任务</option>' + tasks.map((task) => `<option value="${task.id}">${escapeHTML(task.name)}</option>`).join('')
  filter.value = previous
}

function renderExecutions() {
  $('execution-total').textContent = `${state.total} 条记录`
  $('page-label').textContent = `第 ${state.page} 页`
  $('prev-page').disabled = state.page <= 1
  $('next-page').disabled = state.page * 10 >= state.total
  $('executions-table').hidden = state.executions.length === 0
  $('executions-empty').hidden = state.executions.length > 0
  $('execution-rows').innerHTML = state.executions.map((execution) => {
    const cancel = ['RUNNING', 'RETRYING'].includes(execution.status) ? `<button data-action="cancel">取消</button>` : ''
    return `<tr data-id="${escapeHTML(execution.id)}"><td><span class="mono execution-id" title="${escapeHTML(execution.id)}">${escapeHTML(execution.id)}</span>${execution.error_message ? `<small class="error-text">${escapeHTML(execution.error_message)}</small>` : ''}</td><td>${escapeHTML(taskName(execution.task_definition_id))}</td><td>${badge(execution.status)}</td><td class="muted">${escapeHTML(formatTime(execution.started_at_ms))}</td><td class="muted">${escapeHTML(formatTime(execution.finished_at_ms))}</td><td><div class="row-actions">${cancel}</div></td></tr>`
  }).join('')
}

async function loadTasks() {
  try {
    const result = await requestJSON('/task-definitions')
    state.tasks = result.items || []
    renderTasks()
    $('connection-dot').classList.add('online')
    $('connection-label').textContent = '服务已连接'
  } catch (error) {
    $('connection-dot').classList.remove('online')
    $('connection-label').textContent = '连接失败'
    toast(error.message, true)
  }
}

async function loadExecutions() {
  try {
    const query = new URLSearchParams({ page: state.page, page_size: 10 })
    if ($('execution-task').value) query.set('task_definition_id', $('execution-task').value)
    if ($('execution-status').value) query.set('status', $('execution-status').value)
    const result = await requestJSON(`/task-executions?${query}`)
    state.executions = result.items || []
    state.total = result.total || 0
    renderExecutions()
  } catch (error) {
    toast(error.message, true)
  }
}

function toggleSchedule() {
  const cron = $('task-schedule').value === 'CRON'
  $('cron-field').hidden = !cron
  $('timezone-row').hidden = !cron
  $('delayed-field').hidden = cron
}

function openForm(task = null) {
  state.editing = task
  $('task-form').reset()
  $('form-error').hidden = true
  $('dialog-title').textContent = task ? `编辑任务 #${task.id}` : '新建任务'
  $('save-task').textContent = task ? '保存修改' : '创建任务'
  $('task-name').value = task?.name || ''
  $('task-description').value = task?.description || ''
  $('task-schedule').value = task?.schedule_type || 'CRON'
  $('task-cron').value = task?.cron_expression || '* * * * *'
  $('task-timezone').value = task?.timezone || 'Asia/Shanghai'
  $('task-execute-at').value = task?.execute_at_ms ? new Date(task.execute_at_ms - new Date().getTimezoneOffset() * 60000).toISOString().slice(0, 16) : ''
  $('task-payload').value = JSON.stringify(task?.payload ?? { message: 'hello' }, null, 2)
  $('task-concurrency').value = task?.worker.concurrency ?? 1
  $('task-retry').value = task?.worker.max_retry ?? 3
  toggleSchedule()
  $('task-dialog').showModal()
  $('task-name').focus()
}

async function saveForm(event) {
  event.preventDefault()
  const button = $('save-task')
  const errorBox = $('form-error')
  errorBox.hidden = true
  try {
    const values = Object.fromEntries(new FormData($('task-form')))
    const { definition, worker } = taskFormInput(values)
    button.disabled = true
    if (state.editing) {
      const id = state.editing.id
      await requestJSON(`/task-definitions/${id}`, { method: 'PATCH', body: definition })
      if (worker.concurrency !== state.editing.worker.concurrency || worker.max_retry !== state.editing.worker.max_retry) {
        await requestJSON(`/task-definitions/${id}/worker`, { method: 'PATCH', body: worker })
      }
      toast('任务已更新')
    } else {
      await requestJSON('/task-definitions', { method: 'POST', body: { ...definition, worker: { ...worker, status: 'RUNNING' } } })
      toast('任务已创建，发布后即可执行')
    }
    $('task-dialog').close()
    await loadTasks()
  } catch (error) {
    errorBox.textContent = error.message
    errorBox.hidden = false
    await loadTasks()
  } finally {
    button.disabled = false
  }
}

function showDetail(task) {
  $('detail-title').textContent = task.name
  const fields = [
    ['任务 ID', `#${task.id}`], ['状态', statusLabel[task.status]],
    ['调度类型', task.schedule_type === 'CRON' ? 'Cron' : '单次延时'],
    ['调度计划', task.schedule_type === 'CRON' ? `${task.cron_expression} · ${task.timezone}` : formatTime(task.execute_at_ms)],
    ['Worker', `${task.worker.handler_type} · ${statusLabel[task.worker.status]} · 并发 ${task.worker.concurrency} · 重试 ${task.worker.max_retry}`],
    ['创建时间', formatTime(task.created_at_ms)], ['更新时间', formatTime(task.updated_at_ms)],
  ]
  $('detail-content').innerHTML = `<dl class="detail-list">${fields.map(([label, value]) => `<div><dt>${escapeHTML(label)}</dt><dd>${escapeHTML(value)}</dd></div>`).join('')}</dl><h3>Payload</h3><pre class="payload-preview">${escapeHTML(JSON.stringify(task.payload, null, 2))}</pre>`
  $('detail-dialog').showModal()
}

async function taskAction(action, task) {
  if (action === 'detail') return showDetail(task)
  if (action === 'edit') return openForm(task)
  if (action === 'logs') {
    $('execution-task').value = task.id
    state.page = 1
    switchTab('executions')
    return
  }
  const actions = {
    publish: ['publish', '任务已发布'], pause: ['pause', '任务已暂停'], resume: ['resume', '任务已恢复'],
    'start-worker': ['worker/start', 'Worker 已启动'], 'stop-worker': ['worker/stop', 'Worker 已停止'],
  }
  try {
    if (action === 'run') {
      const result = await requestJSON(`/task-definitions/${task.id}/run`, { method: 'POST' })
      toast(`任务已投递：${result.execution_id}`)
      $('execution-task').value = task.id
      state.page = 1
      switchTab('executions')
      setTimeout(() => { if (state.tab === 'executions') void loadExecutions() }, 2500)
      return
    }
    const [path, message] = actions[action]
    await requestJSON(`/task-definitions/${task.id}/${path}`, { method: 'POST' })
    toast(message)
    await loadTasks()
  } catch (error) {
    toast(error.message, true)
  }
}

function boot() {
  $('new-task').addEventListener('click', () => openForm())
  $('empty-new-task').addEventListener('click', () => openForm())
  $('close-dialog').addEventListener('click', () => $('task-dialog').close())
  $('cancel-dialog').addEventListener('click', () => $('task-dialog').close())
  $('close-detail').addEventListener('click', () => $('detail-dialog').close())
  $('detail-done').addEventListener('click', () => $('detail-dialog').close())
  $('task-schedule').addEventListener('change', toggleSchedule)
  $('task-form').addEventListener('submit', saveForm)
  $('refresh').addEventListener('click', () => { void loadTasks(); if (state.tab === 'executions') void loadExecutions() })
  document.querySelectorAll('.tab').forEach((button) => button.addEventListener('click', () => switchTab(button.dataset.tab)))
  $('execution-task').addEventListener('change', () => { state.page = 1; void loadExecutions() })
  $('execution-status').addEventListener('change', () => { state.page = 1; void loadExecutions() })
  $('prev-page').addEventListener('click', () => { state.page--; void loadExecutions() })
  $('next-page').addEventListener('click', () => { state.page++; void loadExecutions() })
  $('task-rows').addEventListener('click', (event) => {
    const button = event.target.closest('button[data-action]')
    if (!button) return
    const task = state.tasks.find((item) => item.id === Number(button.closest('tr').dataset.id))
    if (task) void taskAction(button.dataset.action, task)
  })
  $('execution-rows').addEventListener('click', async (event) => {
    const button = event.target.closest('button[data-action="cancel"]')
    if (!button || !window.confirm('取消这次执行？')) return
    try {
      await requestJSON(`/task-executions/${encodeURIComponent(button.closest('tr').dataset.id)}/cancel`, { method: 'POST' })
      toast('执行已取消')
      await loadExecutions()
    } catch (error) { toast(error.message, true) }
  })
  void loadTasks()
}

if (typeof document !== 'undefined') boot()
