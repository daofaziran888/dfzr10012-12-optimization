import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/state'
const MANIFEST_ENDPOINT = '/api/state/manifests'
const columns = [
  "配置编号",
  "后端类型",
  "存储路径",
  "状态大小",
  "过期策略",
  "清理间隔",
  "快照频率",
  "配置状态",
  "快照状态"
]
const actions = [
  "启用后端",
  "暂停后端",
  "关闭后端",
  "清理快照"
]
const statuses = [
  "草稿",
  "已启用",
  "已暂停",
  "已关闭"
]

type Row = Record<string, string | number | null>
type Manifest = Record<string, any>

export default function StateView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [manifests, setManifests] = useState<Manifest[]>([])
  const [selectedManifest, setSelectedManifest] = useState<Manifest | null>(null)
  const [selectedIds, setSelectedIds] = useState<number[]>([])
  const [consumers, setConsumers] = useState<Manifest[]>([])
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const [listResponse, manifestResponse, consumerResponse] = await Promise.all([
        request(`${ENDPOINT}?${query}`),
        request(`${MANIFEST_ENDPOINT}?size=20`),
        request(`${MANIFEST_ENDPOINT}/consumers`),
      ])
      if (!listResponse.ok) {
        throw new Error('后端配置列表读取失败')
      }
      const payload = await listResponse.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
      if (manifestResponse.ok) {
        const manifestPayload = await manifestResponse.json()
        setManifests(manifestPayload.items ?? [])
      }
      if (consumerResponse.ok) {
        setConsumers(await consumerResponse.json())
      }
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '状态后端管理列表读取失败')
    }
  }, [filters])

  useEffect(() => {
    void reload()
  }, [reload])

  function toggleRow(row: Row) {
    const id = Number(row.id)
    setSelectedIds((prev) => (prev.includes(id) ? prev.filter((item) => item !== id) : [...prev, id]))
  }

  async function post(path: string, values: Record<string, unknown>) {
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(path, {
        method: 'POST',
        body: JSON.stringify({ values }),
      })
      const payload = await response.json()
      if (!response.ok || payload.ok === false) {
        throw new Error(payload.detail ?? payload.message ?? '状态后端管理动作未生效，请稍后重试')
      }
      setNotice(String(payload.message ?? '操作已完成'))
      if (payload.entry?.清单列表) {
        setSelectedManifest(payload.entry)
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '状态后端管理操作失败')
    }
  }

  async function runAction(action: string, row: Row) {
    await post(`${ENDPOINT}/${row.id}/actions`, { action })
  }

  async function runBatchCleanup() {
    // 勾选的配置编号参与一次批量清理；未勾选时整批按当前筛选结果提交。
    const stateIds = selectedIds.length
      ? selectedIds
      : rows.map((row) => Number(row.id)).filter((id) => Number.isFinite(id))
    if (!stateIds.length) {
      setErrorMessage('请先勾选需要清理的状态快照')
      return
    }
    await post(`${MANIFEST_ENDPOINT}/cleanup`, { state_ids: stateIds, 预演: false })
  }

  async function retryManifest(manifest: Manifest) {
    await post(`${MANIFEST_ENDPOINT}/${manifest.id}/retry`, {})
  }

  async function syncManifest(manifest: Manifest) {
    await post(`${MANIFEST_ENDPOINT}/${manifest.id}/sync`, {})
  }

  async function openManifest(manifest: Manifest) {
    try {
      const response = await request(`${MANIFEST_ENDPOINT}/${manifest.id}`)
      if (!response.ok) {
        throw new Error('状态快照数据清单读取失败')
      }
      setSelectedManifest(await response.json())
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '状态快照数据清单读取失败')
    }
  }

  const summary = selectedManifest

  return (
    <section className="page" data-module="state">
      <header className="page-head">
        <div>
          <h2>状态后端管理管理</h2>
          <p className="page-desc">维护状态后端的类型选择、存储配置与过期策略；清理过期快照前先校验检查点引用，被恢复检查点引用的目录整组保留。</p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => void runBatchCleanup()}>
            批量清理过期快照
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出状态后端管理清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        <article className="stat-card">
          <span className="stat-label">后端配置总数</span>
          <strong className="stat-value">{total}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">已勾选快照</span>
          <strong className="stat-value">{selectedIds.length}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">状态快照数据清单</span>
          <strong className="stat-value">{manifests.length}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">待同步版本</span>
          <strong className="stat-value">{consumers.reduce((sum, item) => sum + Number(item.待同步 ?? 0), 0)}</strong>
        </article>
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            <th>选择</th>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              <td>
                <input
                  type="checkbox"
                  checked={selectedIds.includes(Number(row.id))}
                  onChange={() => toggleRow(row)}
                />
              </td>
              {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
              <td className="row-actions">
                {actions.map((action) => (
                  <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                    {action}
                  </button>
                ))}
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 2}>暂无状态后端管理数据，可先登记后端配置</td>
            </tr>
          )}
        </tbody>
      </table>

      {summary && (
        <section className="manifest-panel">
          <header className="page-head">
            <div>
              <h3>状态快照数据清单 {summary.清单编号 ?? ''}</h3>
              <p className="page-desc">
                删除 {summary.整组进度?.已删除 ?? 0} 项 / 保留 {summary.整组进度?.已保留 ?? 0} 项 /
                失败 {summary.整组进度?.失败 ?? 0} 项，受保护大小 {summary.受保护大小 ?? '0B'}
                {summary.版本标识 !== undefined ? `，版本标识 ${summary.版本标识}` : ''}
              </p>
            </div>
            <button className="btn ghost" type="button" onClick={() => setSelectedManifest(null)}>关闭</button>
          </header>
          {summary.清单列表 && (
            <table className="data-table">
              <thead>
                <tr><th>清单编号</th><th>存储路径</th><th>整组进度</th><th>受保护大小</th><th>版本标识</th><th>操作</th></tr>
              </thead>
              <tbody>
                {(summary.清单列表 as Manifest[]).map((item) => (
                  <tr key={String(item.id)}>
                    <td>{item.清单编号}</td>
                    <td>{item.存储路径}</td>
                    <td>
                      删除 {item.整组进度?.已删除 ?? 0} / 保留 {item.整组进度?.已保留 ?? 0} / 失败 {item.整组进度?.失败 ?? 0}
                    </td>
                    <td>{item.受保护大小}</td>
                    <td>{item.版本标识}</td>
                    <td className="row-actions">
                      <button className="link" type="button" onClick={() => void openManifest(item)}>明细</button>
                      <button className="link" type="button" onClick={() => void retryManifest(item)}>失败续跑</button>
                      <button className="link" type="button" onClick={() => void syncManifest(item)}>同步消费侧</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          <table className="data-table">
            <thead>
              <tr><th>删除结果</th><th>受保护目录</th><th>失败项</th></tr>
            </thead>
            <tbody>
              <tr>
                <td>
                  {(summary.删除结果 ?? []).map((item: Manifest) => (
                    <div key={String(item.路径)}>{item.路径} · {item.结果}</div>
                  ))}
                  {!(summary.删除结果 ?? []).length && '—'}
                </td>
                <td>
                  {(summary.保护项 ?? []).map((item: Manifest) => (
                    <div key={String(item.路径)}>{item.路径} · {item.大小}</div>
                  ))}
                  {!(summary.保护项 ?? []).length && '—'}
                </td>
                <td>
                  {(summary.失败项 ?? []).map((item: Manifest) => (
                    <div key={String(item.路径)} className="error-text">{item.路径} · {item.结果}</div>
                  ))}
                  {!(summary.失败项 ?? []).length && '—'}
                </td>
              </tr>
            </tbody>
          </table>
        </section>
      )}

      <footer className="page-foot">
        <span>共 {total} 条状态后端管理记录</span>
        {notice && <span>{notice}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
