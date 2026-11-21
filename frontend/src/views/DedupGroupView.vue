<template>
  <section class="page" data-module="dedup_group">
    <header class="page-head">
      <div>
        <h2>去重组对照视图</h2>
        <p class="page-desc">
          按去重组还原被淘汰日志的原文与本次采用的字段版本，可从组内发起回溯重放。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="load-groups" type="button" @click="reload">刷新分组</button>
      </div>
    </header>

    <p v-if="blocker" class="blocker-banner" data-testid="downstream-blocker">
      ⚠ {{ blocker }}
    </p>

    <div class="compare-layout">
      <aside class="group-list">
        <form class="filter-bar" @submit.prevent="reload">
          <label class="filter-item">
            <span>关键字</span>
            <input v-model="filters.keyword" data-testid="filter-keyword" placeholder="组号 / 去重键 / 批次" />
          </label>
          <label class="filter-item">
            <span>组状态</span>
            <select v-model="filters.state" data-testid="filter-state">
              <option value="">全部</option>
              <option value="已确认">已确认</option>
              <option value="已被替代">已被替代</option>
            </select>
          </label>
          <button class="btn" type="submit">查询</button>
        </form>
        <button
          v-for="group in groups"
          :key="group.id"
          class="group-card"
          :class="{ selected: group.id === selectedId }"
          :data-testid="`group-${group.id}`"
          type="button"
          @click="selectGroup(group.id)"
        >
          <div class="group-card-head">
            <strong>{{ group.group_no }}</strong>
            <span class="badge" :class="group.active ? 'badge-ok' : 'badge-muted'">{{ group.state }}</span>
          </div>
          <div class="group-key">{{ group.key_value }}</div>
          <div class="group-meta">
            采纳 {{ group.member_count - group.eliminated_count }} · 淘汰 {{ group.eliminated_count }}
          </div>
        </button>
        <p v-if="!groups.length" class="empty-state">暂无去重组，可先到去重引擎执行去重</p>
      </aside>

      <section v-if="group" class="group-detail" :data-group="group.group_no">
        <header class="detail-head">
          <div>
            <h3>{{ group.group_no }} 去重组</h3>
            <p class="page-desc">
              去重键 <code>{{ group.key_fields }}</code> = {{ group.key_value }} ·
              保留策略 {{ group.retain_strategy }} · 批次 {{ group.batch_no }}
            </p>
          </div>
          <span class="badge" :class="group.active ? 'badge-ok' : 'badge-muted'">{{ group.state }}</span>
        </header>

        <h4>本次采用的字段版本</h4>
        <table class="data-table compact" data-testid="field-versions">
          <thead>
            <tr><th>字段</th><th>段位</th><th>解析规则</th><th>是否去重键</th></tr>
          </thead>
          <tbody>
            <tr v-for="version in versionRows" :key="version.field">
              <td>{{ version.field }}</td>
              <td>{{ version.source_position ?? '—' }}</td>
              <td>{{ version.parser_rule || '—' }}</td>
              <td>{{ version.key_field ? '是' : '否' }}</td>
            </tr>
            <tr v-if="!versionRows.length"><td colspan="4" class="empty-state">暂无字段版本记录</td></tr>
          </tbody>
        </table>

        <table class="data-table" data-testid="group-members">
          <thead>
            <tr>
              <th>判定</th><th>原始记录</th><th>原始行号</th><th>原始正文</th>
              <th>字段集</th><th>解析规则</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="member in group.members"
              :key="member.entity_id"
              :data-testid="`member-${member.entity_id}`"
              :class="{ 'row-eliminated': member.eliminated }"
              :data-verdict="member.eliminated ? '淘汰' : '胜出'"
            >
              <td>
                <span class="badge" :class="member.eliminated ? 'badge-danger' : 'badge-ok'">
                  {{ member.eliminated ? '淘汰' : '胜出' }}
                </span>
              </td>
              <td>#{{ member.raw_record_id }}</td>
              <td>{{ member.line_no }}</td>
              <td class="mono">{{ member.raw_body }}</td>
              <td class="mono">{{ formatFields(member.fields) }}</td>
              <td>{{ member.parser_rule || '—' }}</td>
            </tr>
          </tbody>
        </table>
        <p v-if="group.eliminated_count" class="hint">
          被淘汰日志：{{ group.eliminated.map((m) => `#${m.raw_record_id}`).join('、') }}
        </p>

        <section class="replay-box" data-testid="replay-box">
          <h4>回溯重放</h4>
          <form class="analysis-form" @submit.prevent="previewReplay">
            <label>
              <span>更改解析字段</span>
              <select v-model="form.field" data-testid="replay-field">
                <option v-for="field in fieldNames" :key="field" :value="field">{{ field }}</option>
              </select>
            </label>
            <label>
              <span>改取原文段位</span>
              <input v-model.number="form.position" type="number" min="0" data-testid="replay-position" />
            </label>
            <button class="btn primary" data-testid="preview-replay" type="submit">预览影响范围</button>
          </form>

          <div v-if="preview" class="scope-panel" data-testid="replay-scope">
            <p>
              受影响去重组 <strong>{{ preview.affected_group_count }}</strong> 个（{{ preview.source_groups.join('、') }}），
              待复算原始日志 <strong>{{ preview.record_count }}</strong> 条，涉及 {{ preview.raw_record_count }} 份原文。
            </p>
            <p v-if="preview.cross_group_records.length" class="hint">
              其中 {{ preview.cross_group_records.length }} 条来自跨组重合（新键与组外已使用日志相同），需一并复算。
            </p>
            <table class="data-table compact" data-testid="replay-records">
              <thead>
                <tr><th>源组</th><th>原始记录</th><th>行号</th><th>原判定</th><th>旧键 → 新键</th><th>跨组</th></tr>
              </thead>
              <tbody>
                <tr v-for="record in preview.records" :key="`${record.source_group_id}-${record.raw_record_id}`">
                  <td>{{ record.source_group_no }}</td>
                  <td>#{{ record.raw_record_id }}</td>
                  <td>{{ record.line_no }}</td>
                  <td>{{ record.was_winner ? '胜出' : '淘汰' }}</td>
                  <td class="mono">
                    {{ record.current_key }} → {{ record.recomputed_key }}
                    <span v-if="record.key_changed" class="badge badge-warn">键变化</span>
                  </td>
                  <td>{{ record.cross_group ? record.cross_group_nos.join('、') : '—' }}</td>
                </tr>
              </tbody>
            </table>
            <div class="page-actions">
              <button class="btn primary" data-testid="start-replay" type="button" @click="startReplay()">发起重放</button>
              <button class="btn" data-testid="start-replay-interrupt" type="button" @click="startReplay(1)">
                模拟中断重放
              </button>
            </div>
          </div>

          <div v-if="task" class="task-panel" data-testid="replay-task" :data-phase="task.phase">
            <p>
              {{ task.task_no }}：<strong>{{ task.phase }}</strong> ·
              草稿 {{ task.draft.length }}/{{ task.records.length }} 条 ·
              规则版本 {{ task.new_rule_version }}
            </p>
            <p v-if="task.phase === '已中断'" class="error-text">
              重放中断，草稿未提交；恢复并确认前下游读取被阻断。
            </p>
            <p v-if="task.phase === '待确认'" class="hint">
              重放结果已在草稿区，确认前下游（聚合 / 导出）不能读取半个去重组。
            </p>
            <div class="page-actions">
              <button
                v-if="task.phase === '已中断'"
                class="btn"
                data-testid="resume-replay"
                type="button"
                @click="resumeReplay"
              >恢复重放</button>
              <button
                v-if="task.phase === '待确认'"
                class="btn primary"
                data-testid="confirm-replay"
                type="button"
                @click="confirmReplay"
              >确认并回写修正清单</button>
            </div>
          </div>

          <div v-if="corrections.length" class="correction-panel" data-testid="correction-list">
            <h4>修正清单</h4>
            <table class="data-table compact">
              <thead>
                <tr><th>原始记录</th><th>行号</th><th>源组</th><th>旧键</th><th>新键</th><th>判定变化</th><th>新组</th></tr>
              </thead>
              <tbody>
                <tr v-for="item in corrections" :key="item.raw_record_id" :data-testid="`correction-${item.raw_record_id}`">
                  <td>#{{ item.raw_record_id }}</td>
                  <td>{{ item.line_no }}</td>
                  <td>{{ item.source_group_no }}</td>
                  <td class="mono">{{ item.old_key }}</td>
                  <td class="mono">{{ item.new_key }}</td>
                  <td>{{ item.old_verdict }} → {{ item.new_verdict }}</td>
                  <td>{{ item.new_group_no || '—' }}</td>
                </tr>
              </tbody>
            </table>
            <h4>重放后胜出组</h4>
            <ul class="winner-list">
              <li v-for="winner in winnerGroups" :key="winner.id" :data-testid="`winner-group-${winner.id}`">
                <strong>{{ winner.group_no }}</strong> {{ winner.key_value }} —
                胜出 #{{ winner.winner?.raw_record_id ?? '—' }}，淘汰
                {{ winner.eliminated.map((m) => `#${m.raw_record_id}`).join('、') || '无' }}
              </li>
            </ul>
          </div>
        </section>
      </section>

      <section v-else class="group-detail empty-state">从左侧选择一个去重组查看对照</section>
    </div>

    <footer class="page-foot">
      <span>共 {{ groups.length }} 个去重组</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'

import { request } from '@/api/client'

type Member = {
  entity_id: number
  raw_record_id: string
  line_no: string
  raw_body: string
  fields: Record<string, string>
  field_versions: Record<string, FieldVersion>
  parser_rule: string
  eliminated: boolean
}

type FieldVersion = {
  field: string
  source_position: number | string
  parser_rule: string
  key_field: boolean
}

type Group = {
  id: number
  group_no: string
  key_fields: string
  key_value: string
  retain_strategy: string
  batch_no: string
  state: string
  active: boolean
  member_count: number
  eliminated_count: number
  field_versions: Record<string, FieldVersion>
  winner: Member | null
  members: Member[]
  eliminated: Member[]
}

type ReplayRecord = {
  source_group_id: number
  source_group_no: string
  raw_record_id: string
  line_no: string
  was_winner: boolean
  current_key: string
  recomputed_key: string
  key_changed: boolean
  cross_group: boolean
  cross_group_nos: string[]
}

type Preview = {
  affected_group_count: number
  source_groups: string[]
  record_count: number
  raw_record_count: number
  cross_group_records: ReplayRecord[]
  records: ReplayRecord[]
  new_rule_version: string
}

type Task = {
  id: number
  task_no: string
  phase: string
  new_rule_version: string
  draft: unknown[]
  records: ReplayRecord[]
}

type Correction = {
  raw_record_id: string
  line_no: string
  source_group_no: string
  old_key: string
  new_key: string
  old_verdict: string
  new_verdict: string
  new_group_no: string
}

const route = useRoute()
const groups = ref<Group[]>([])
const selectedId = ref<number | null>(null)
const blocker = ref('')
const errorMessage = ref('')
const filters = ref({ keyword: '', state: '' })
const form = ref({ field: 'level', position: 2 })
const preview = ref<Preview | null>(null)
const task = ref<Task | null>(null)
const corrections = ref<Correction[]>([])
const winnerGroups = ref<Group[]>([])

const group = computed(() => groups.value.find((item) => item.id === selectedId.value) ?? null)
const fieldNames = computed(() => Object.keys(group.value?.field_versions ?? {}))
const versionRows = computed(() => Object.values(group.value?.field_versions ?? {}))

function formatFields(fields: Record<string, string>): string {
  return Object.entries(fields).map(([key, value]) => `${key}=${value}`).join(' | ')
}

async function json<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await request(path, init)
  if (!response.ok) throw new Error(`接口返回 ${response.status}`)
  return (await response.json()) as T
}

async function reload() {
  errorMessage.value = ''
  try {
    const query = new URLSearchParams({
      keyword: filters.value.keyword,
      state: filters.value.state,
      include_pending: 'true',
    }).toString()
    const payload = await json<{ items: Group[]; downstream_blocker: string | null }>(
      `/api/pipeline/dedup-groups?${query}`,
    )
    groups.value = payload.items
    blocker.value = payload.downstream_blocker ?? ''
    const groupQuery = route.query.group
    const wanted = Number(Array.isArray(groupQuery) ? groupQuery[0] : groupQuery) || 0
    if (wanted && groups.value.some((item) => item.id === wanted)) {
      if (selectedId.value !== wanted) selectGroup(wanted)
      return
    }
    if (selectedId.value === null || !groups.value.some((item) => item.id === selectedId.value)) {
      selectedId.value = groups.value[0]?.id ?? null
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '去重组读取失败'
  }
}

function selectGroup(id: number) {
  selectedId.value = id
  preview.value = null
  task.value = null
  corrections.value = []
  winnerGroups.value = []
  const versions = groups.value.find((item) => item.id === id)?.field_versions ?? {}
  form.value.field = Object.keys(versions)[0] ?? 'level'
}

async function previewReplay() {
  if (!group.value) return
  errorMessage.value = ''
  try {
    preview.value = await json<Preview>(`/api/pipeline/dedup-groups/${group.value.id}/replay/preview`, {
      method: 'POST',
      body: JSON.stringify(form.value),
    })
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '影响范围计算失败'
  }
}

async function startReplay(interruptAfter?: number) {
  if (!group.value) return
  errorMessage.value = ''
  try {
    const payload = await json<{ ok: boolean; message: string; task: Task }>(
      `/api/pipeline/dedup-groups/${group.value.id}/replay`,
      {
        method: 'POST',
        body: JSON.stringify({ ...form.value, interrupt_after: interruptAfter ?? null }),
      },
    )
    task.value = payload.task
    corrections.value = []
    winnerGroups.value = []
    if (!payload.ok) errorMessage.value = payload.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重放发起失败'
  }
}

async function resumeReplay() {
  if (!task.value) return
  try {
    const payload = await json<{ ok: boolean; message: string; task: Task }>(
      `/api/pipeline/replays/${task.value.id}/resume`,
      { method: 'POST', body: JSON.stringify({}) },
    )
    task.value = payload.task
    if (!payload.ok) errorMessage.value = payload.message
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重放恢复失败'
  }
}

async function confirmReplay() {
  if (!task.value) return
  try {
    const payload = await json<{
      ok: boolean
      message: string
      task: Task
      corrections: Correction[]
      winner_groups: Group[]
    }>(`/api/pipeline/replays/${task.value.id}/confirm`, { method: 'POST', body: JSON.stringify({}) })
    task.value = payload.task
    corrections.value = payload.corrections
    winnerGroups.value = payload.winner_groups
    if (!payload.ok) errorMessage.value = payload.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重放确认失败'
  }
}

onMounted(async () => {
  await reload()
  if (!groups.value.length) {
    try {
      await json('/api/pipeline/dedup-groups/demo', { method: 'POST', body: JSON.stringify({}) })
      await reload()
    } catch {
      /* 演示数据可选，失败时保留空态 */
    }
  }
})
</script>

<style scoped>
.compare-layout { display: grid; grid-template-columns: 260px 1fr; gap: 16px; }
.group-list { display: flex; flex-direction: column; gap: 8px; }
.group-card { text-align: left; border: 1px solid var(--border); background: #fff; border-radius: 8px; padding: 10px; cursor: pointer; }
.group-card.selected { border-color: var(--brand); box-shadow: 0 0 0 2px rgb(31 111 235 / 20%); }
.group-card-head { display: flex; justify-content: space-between; align-items: center; }
.group-key { font-family: monospace; font-size: 12px; margin: 6px 0; word-break: break-all; }
.group-meta { color: var(--muted); font-size: 12px; }
.group-detail { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 16px; min-height: 320px; }
.detail-head { display: flex; justify-content: space-between; align-items: flex-start; }
.replay-box { margin-top: 20px; border-top: 1px dashed var(--border); padding-top: 12px; }
.badge { display: inline-block; border-radius: 999px; padding: 1px 8px; font-size: 12px; }
.badge-ok { background: #dcfce7; color: #166534; }
.badge-danger { background: #fee2e2; color: #991b1b; }
.badge-warn { background: #fef3c7; color: #92400e; }
.badge-muted { background: #e5e7eb; color: #475569; }
.row-eliminated { background: #fff7f7; }
.compact td, .compact th { padding: 5px 8px; }
.mono { font-family: monospace; font-size: 12px; word-break: break-all; }
.hint { color: var(--muted); font-size: 12px; }
.blocker-banner { background: #fef3c7; border: 1px solid #fcd34d; color: #92400e; padding: 8px 12px; border-radius: 6px; }
.scope-panel, .task-panel, .correction-panel { margin-top: 12px; padding: 12px; border: 1px solid var(--border); border-radius: 8px; background: #fafbfc; }
.winner-list { margin: 0; padding-left: 18px; font-size: 13px; }
</style>
