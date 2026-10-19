import test from 'node:test'
import assert from 'node:assert/strict'
import { requestJSON, taskFormInput } from './app.mjs'

const cron = {
  name: '  daily job  ', description: 'test', schedule_type: 'CRON',
  cron_expression: '* * * * *', timezone: 'Asia/Shanghai',
  payload: '{"message":"hello","nested_key":{"tenant_id":1}}',
  concurrency: '2', max_retry: '0', execute_at: '',
}

test('Cron 表单保留原始 payload 键名与 Worker 参数', () => {
  const result = taskFormInput(cron)
  assert.equal(result.definition.name, 'daily job')
  assert.equal(result.definition.payload.nested_key.tenant_id, 1)
  assert.equal(result.worker.concurrency, 2)
  assert.equal(result.worker.max_retry, 0)
  assert.equal(result.worker.handler_type, 'log')
})

test('延时任务转换为毫秒时间戳', () => {
  const result = taskFormInput({ ...cron, schedule_type: 'DELAYED', execute_at: '2030-01-01T10:00' }, 0)
  assert.equal(result.definition.execute_at_ms, new Date('2030-01-01T10:00').getTime())
  assert.equal('cron_expression' in result.definition, false)
})

test('非法输入在请求前被拒绝', () => {
  assert.throws(() => taskFormInput({ ...cron, payload: '[1]' }), /JSON 对象/)
  assert.throws(() => taskFormInput({ ...cron, concurrency: '11' }), /并发/)
  assert.throws(() => taskFormInput({ ...cron, schedule_type: 'DELAYED', execute_at: '2020-01-01T00:00' }), /晚于当前时间/)
})

test('API 请求保留 snake_case 字段并解析服务端错误', async () => {
  let sent
  const fetcher = async (url, options) => {
    sent = { url, options }
    return { ok: true, status: 201, json: async () => ({ id: 1 }) }
  }
  const result = await requestJSON('/task-definitions', { method: 'POST', body: { schedule_type: 'CRON' } }, fetcher)
  assert.deepEqual(result, { id: 1 })
  assert.equal(sent.url, '/api/v1/task-definitions')
  assert.deepEqual(JSON.parse(sent.options.body), { schedule_type: 'CRON' })

  await assert.rejects(
    () => requestJSON('/task-definitions', {}, async () => ({ ok: false, status: 400, json: async () => ({ error: 'invalid cron' }) })),
    /invalid cron/,
  )
})
