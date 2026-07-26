import {
  ChangeDetectionStrategy,
  Component,
  Injectable,
  OnDestroy,
  OnInit,
  computed,
  inject,
  signal
} from '@angular/core';

import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { HttpClient } from '@angular/common/http';
import { EMPTY, Subject, catchError, forkJoin, interval, of, takeUntil } from 'rxjs';

/* ============================================================
   Runtime Settings
   ============================================================ */

export interface AppSettings {
  sqlFlowQueryApiUrl: string;
  sqlFlowAdminApiUrl: string;
}

@Injectable({ providedIn: 'root' })
export class AppSettingsService {
  private readonly settingsSignal = signal<AppSettings | null>(null);

  async load(): Promise<void> {
    const response = await fetch('/appsettings.json', { cache: 'no-store' });

    if (!response.ok) {
      throw new Error(`Could not load appsettings.json. Status: ${response.status}`);
    }

    const settings = await response.json() as AppSettings;

    if (!settings.sqlFlowQueryApiUrl) {
      throw new Error('Missing setting: sqlFlowQueryApiUrl');
    }

    if (!settings.sqlFlowAdminApiUrl) {
      throw new Error('Missing setting: sqlFlowAdminApiUrl');
    }

    this.settingsSignal.set(settings);
  }

  get sqlFlowDashboardApiUrl(): string {
    const settings = this.settingsSignal();
    if (!settings) throw new Error('App settings have not been loaded.');
    return settings.sqlFlowQueryApiUrl;
  }

  get sqlFlowAdminApiUrl(): string {
    const settings = this.settingsSignal();
    if (!settings) throw new Error('App settings have not been loaded.');
    return settings.sqlFlowAdminApiUrl;
  }
}

/* ============================================================
   Models
   ============================================================ */

export interface QueueStatItem {
  queueName: string;
  state: string;
  count: number;
}

export interface ThroughputBucketItem {
  timeBucket: string;
  queueName: string;
  completedCount: number;
  failedCount: number;
  avgDurationMs: number;
}

export interface TaskPercentileItem {
  taskName: string;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
}

export interface DatabaseHealthItem {
  engineName: string;
  queueName: string;
  tasksTableBytes: number;
  runsTableBytes: number;
  activeLocks: number;
}

export interface ActiveWorkerItem {
  queueName: string;
  workerId: string;
  activeRuns: number;
}

export interface QueueWaitTimeItem {
  queueName: string;
  avgWaitTimeMs: number;
  maxWaitTimeMs: number;
}

export interface TaskFailureHotspotItem {
  queueName: string;
  taskName: string;
  failureCount: number;
  lastFailedAt: string;
}

export interface ActiveWaitItem {
  queueName: string;
  eventName: string;
  waitingCount: number;
  oldestWaitAt: string | null;
}

export interface QueueBacklogItem {
  queueName: string;
  pendingCount: number;
  oldestPendingAt: string | null;
}

export interface RetryHotspotItem {
  queueName: string;
  taskId: string;
  taskName: string;
  attempts: number;
  state: string;
}

export interface SlowTaskItem {
  queueName: string;
  taskId: string;
  taskName: string;
  durationMs: number;
  completedAt: string;
}

export interface FailedTaskItem {
  queueName: string;
  taskId: string;
  taskName: string;
  attempts: number;
  runId: string | null;
  failedAt: string | null;
  failureReason: string | null;
}

export interface TaskDetailItem {
  taskId: string;
  queueName: string;
  taskName: string;
  state: string;
  enqueuedAt: string;
  firstStartedAt: string | null;
  completedPayload: string | null;
  params: string | null;
}

export interface TaskSearchResultItem {
  queueName: string;
  taskId: string;
  taskName: string;
  state: string;
  attempts: number;
  runId: string | null;
  enqueuedAt: string;
  lastAttemptAt: string | null;
  failureReason: string | null;
  paramsJson: string | null;
}

export interface TaskSearchFilter {
  queueName: string;
  states?: string[];
  searchTerm?: string;
  minAttempts?: number;
  maxAttempts?: number;
  claimedBy?: string;
  fromDate?: string;
  toDate?: string;
  sortBy: string;
  sortDescending: boolean;
  offset: number;
  limit: number;
}

/* Admin commands */

export interface CreateQueueCommand {
  queueName: string;
  storageMode?: string;
}

export interface CleanupCommand {
  queueName: string;
  ttlSeconds: number;
  limit?: number;
}

export interface EmitEventCommand {
  queueName: string;
  eventName: string;
  payloadJson?: string | null;
}

export interface CompleteRunCommand {
  queueName: string;
  runId: string;
  stateJson?: string | null;
}

export interface FailRunCommand {
  queueName: string;
  runId: string;
  reasonJson: string;
  retryAt?: string | null;
}

export interface ScheduleWakeupCommand {
  queueName: string;
  runId: string;
  wakeAt: string;
}

export interface SetCheckpointCommand {
  queueName: string;
  taskId: string;
  ownerRunId: string;
  stepName: string;
  stateJson: string;
  extendClaimBySeconds?: number | null;
}

export interface ExtendClaimCommand {
  queueName: string;
  runId: string;
  extendBySeconds?: number;
}

export interface CancelTaskCommand {
  queueName: string;
  taskId: string;
}

export interface BulkCancelTasksCommand {
  queueName: string;
  taskIds: string[];
}

export interface ReleaseWorkerClaimsCommand {
  queueName: string;
  workerId: string;
}

export interface AdminResponse {
  message?: string;
  deletedCount?: number;
}

/* ============================================================
   Translations
   ============================================================ */

export interface DashboardTranslations {
  title: string;
  subtitle: string;
  autoRefresh: string;
  refresh: string;
  language: string;
  quickFilter: string;

  monitoring: string;
  analytics: string;
  operations: string;
  administration: string;

  backlog: string;
  activeExecutions: string;
  completed: string;
  failedTasks: string;
  dbStorage: string;

  queue: string;
  state: string;
  count: string;
  taskId: string;
  taskIds: string;
  taskName: string;
  runId: string;
  workerId: string;
  eventName: string;
  action: string;
  inspect: string;
  viewJson: string;
  execute: string;
  close: string;

  queueStateDist: string;
  activeWorkers: string;
  runs: string;
  backlogDepth: string;
  oldest: string;
  pending: string;
  waitTimes: string;
  avgWait: string;
  maxWait: string;

  throughput: string;
  timeBucket: string;
  completedCount: string;
  failedCount: string;
  avgDuration: string;
  latencyPercentiles: string;
  failureHotspots: string;
  fails: string;
  lastFailed: string;
  failedTaskList: string;
  failedAt: string;
  failureReason: string;
  failureReasonParams: string;
  slowTasks: string;
  duration: string;
  completedAt: string;
  retryHotspots: string;
  retries: string;
  eventBlockades: string;
  blocked: string;

  inspectorTab: string;
  queueLabel: string;
  stateLabel: string;
  allStates: string;
  searchPlaceholder: string;
  executeQuery: string;
  attempts: string;
  enqueuedAt: string;

  adminQueues: string;
  adminActions: string;
  adminOperation: string;
  createQueue: string;
  dropQueue: string;
  newQueueName: string;
  storageMode: string;
  unpartitioned: string;
  partitioned: string;

  cancelTask: string;
  bulkCancelTasks: string;
  releaseWorkerClaims: string;
  extendClaim: string;
  wakeRun: string;
  completeRun: string;
  failRun: string;
  emitEvent: string;
  setCheckpoint: string;
  cleanupTasks: string;
  cleanupEvents: string;

  ownerRunId: string;
  stepName: string;
  stateJson: string;
  reasonJson: string;
  payloadJson: string;
  checkpointStateJson: string;
  extendBySeconds: string;
  extendClaimBySeconds: string;
  wakeAt: string;
  retryAt: string;
  ttlSeconds: string;
  limit: string;
  cleanupLimit: string;
  selectQueue: string;
  oneTaskIdPerLine: string;

  prev: string;
  next: string;
  page: string;
  pageSize: string;
  serverLimit: string;
  window: string;
  rows: string;
  loaded: string;
  visible: string;

  parametersJson: string;
  completedPayloadResult: string;

  noQueueData: string;
  noActiveWorkers: string;
  noWaitTimes: string;
  noThroughput: string;
  noLatencyData: string;
  noHotspots: string;
  noFailedTaskList: string;
  noSlowTasks: string;
  noRetryHotspots: string;
  noBlockades: string;
  noMatchingTasks: string;
  noAdminQueues: string;

  deleteQueueConfirm: string;
  releaseClaimsConfirm: string;
  cancelTaskConfirm: string;
}

export const TRANSLATIONS_EN: DashboardTranslations = {
  title: 'SqlFlow System Explorer',
  subtitle: 'Enterprise workflow telemetry and administration console',
  autoRefresh: 'Auto-refresh',
  refresh: 'Refresh',
  language: 'Language',
  quickFilter: 'Quick filter...',

  monitoring: 'Monitoring',
  analytics: 'Analytics',
  operations: 'Operations',
  administration: 'Administration',

  backlog: 'Backlog',
  activeExecutions: 'Running',
  completed: 'Completed',
  failedTasks: 'Failed',
  dbStorage: 'DB Storage',

  queue: 'Queue',
  state: 'State',
  count: 'Count',
  taskId: 'Task ID',
  taskIds: 'Task IDs',
  taskName: 'Task Name',
  runId: 'Run ID',
  workerId: 'Worker ID',
  eventName: 'Event Name',
  action: 'Action',
  inspect: 'Inspect',
  viewJson: 'JSON',
  execute: 'Execute',
  close: 'Close',

  queueStateDist: 'Queue States',
  activeWorkers: 'Active Workers',
  runs: 'Runs',
  backlogDepth: 'Backlog Depth',
  oldest: 'Oldest',
  pending: 'Pending',
  waitTimes: 'Wait Times',
  avgWait: 'Avg Wait',
  maxWait: 'Max Wait',

  throughput: 'Throughput',
  timeBucket: 'Time Bucket',
  completedCount: 'Completed',
  failedCount: 'Failed',
  avgDuration: 'Avg Duration',
  latencyPercentiles: 'Latency Percentiles',
  failureHotspots: 'Failure Hotspots',
  fails: 'Fails',
  lastFailed: 'Last Failed',
  failedTaskList: 'Failed Tasks',
  failedAt: 'Failed At',
  failureReason: 'Failure Reason',
  failureReasonParams: 'Reason / Params',
  slowTasks: 'Slow Tasks',
  duration: 'Duration',
  completedAt: 'Completed At',
  retryHotspots: 'Retry Hotspots',
  retries: 'Retries',
  eventBlockades: 'Event Blockades',
  blocked: 'Blocked',

  inspectorTab: 'Advanced Task Search',
  queueLabel: 'Queue',
  stateLabel: 'State',
  allStates: 'All states',
  searchPlaceholder: 'Task name, error or JSON...',
  executeQuery: 'Run Query',
  attempts: 'Attempts',
  enqueuedAt: 'Enqueued',

  adminQueues: 'Queues',
  adminActions: 'Admin Actions',
  adminOperation: 'Operation',
  createQueue: 'Create Queue',
  dropQueue: 'Drop Queue',
  newQueueName: 'New queue name',
  storageMode: 'Storage Mode',
  unpartitioned: 'unpartitioned',
  partitioned: 'partitioned',

  cancelTask: 'Cancel Task',
  bulkCancelTasks: 'Bulk Cancel Tasks',
  releaseWorkerClaims: 'Release Worker Claims',
  extendClaim: 'Extend Claim',
  wakeRun: 'Wake Run',
  completeRun: 'Force Complete Run',
  failRun: 'Force Fail Run',
  emitEvent: 'Emit Event',
  setCheckpoint: 'Set Checkpoint',
  cleanupTasks: 'Cleanup Tasks',
  cleanupEvents: 'Cleanup Events',

  ownerRunId: 'Owner Run ID',
  stepName: 'Step Name',
  stateJson: 'State JSON',
  reasonJson: 'Reason JSON',
  payloadJson: 'Payload JSON',
  checkpointStateJson: 'Checkpoint State JSON',
  extendBySeconds: 'Extend By Seconds',
  extendClaimBySeconds: 'Extend Claim By Seconds',
  wakeAt: 'Wake At',
  retryAt: 'Retry At',
  ttlSeconds: 'TTL Seconds',
  limit: 'Limit',
  cleanupLimit: 'Cleanup Limit',
  selectQueue: 'Select queue',
  oneTaskIdPerLine: 'One task id per line or comma-separated',

  prev: 'Previous',
  next: 'Next',
  page: 'Page',
  pageSize: 'Page Size',
  serverLimit: 'Limit',
  window: 'Window',
  rows: 'rows',
  loaded: 'loaded',
  visible: 'visible',

  parametersJson: 'Parameters JSON',
  completedPayloadResult: 'Result Payload JSON',

  noQueueData: 'No data.',
  noActiveWorkers: 'No active workers.',
  noWaitTimes: 'No wait time data.',
  noThroughput: 'No throughput data.',
  noLatencyData: 'No latency data.',
  noHotspots: 'No failure hotspots.',
  noFailedTaskList: 'No failed tasks.',
  noSlowTasks: 'No slow tasks.',
  noRetryHotspots: 'No retry hotspots.',
  noBlockades: 'No event blockades.',
  noMatchingTasks: 'No matching tasks.',
  noAdminQueues: 'No queues.',

  deleteQueueConfirm: 'Drop this queue and all associated data?',
  releaseClaimsConfirm: 'Release claims for this worker?',
  cancelTaskConfirm: 'Cancel this task?'
};

export const TRANSLATIONS_DE: DashboardTranslations = {
  ...TRANSLATIONS_EN,
  title: 'SqlFlow System Explorer',
  subtitle: 'Enterprise Workflow-Telemetrie- und Administrationskonsole',
  autoRefresh: 'Auto-Refresh',
  refresh: 'Aktualisieren',
  language: 'Sprache',
  quickFilter: 'Schnellfilter...',

  monitoring: 'Monitoring',
  analytics: 'Analyse',
  operations: 'Operations',
  administration: 'Administration',

  backlog: 'Backlog',
  activeExecutions: 'Laufend',
  completed: 'Erledigt',
  failedTasks: 'Fehler',
  dbStorage: 'DB-Größe',

  state: 'Zustand',
  count: 'Anzahl',
  action: 'Aktion',
  inspect: 'Ansehen',
  execute: 'Ausführen',
  close: 'Schließen',

  queueStateDist: 'Queue-Zustände',
  activeWorkers: 'Aktive Worker',
  backlogDepth: 'Backlog-Tiefe',
  oldest: 'Ältester',
  pending: 'Ausstehend',
  waitTimes: 'Wartezeiten',
  avgWait: 'Ø Wartezeit',
  maxWait: 'Max. Wartezeit',

  throughput: 'Durchsatz',
  timeBucket: 'Zeitfenster',
  completedCount: 'Erledigt',
  failedCount: 'Fehler',
  avgDuration: 'Ø Dauer',
  latencyPercentiles: 'Latenz-Perzentile',
  failureHotspots: 'Fehler-Hotspots',
  fails: 'Fehler',
  lastFailed: 'Zuletzt fehlgeschlagen',
  failedTaskList: 'Fehlgeschlagene Tasks',
  failedAt: 'Fehlgeschlagen am',
  failureReason: 'Fehlergrund',
  failureReasonParams: 'Grund / Parameter',
  slowTasks: 'Langsame Tasks',
  duration: 'Dauer',
  completedAt: 'Abgeschlossen am',
  retryHotspots: 'Retry-Hotspots',
  eventBlockades: 'Event-Blockaden',
  blocked: 'Blockiert',

  inspectorTab: 'Erweiterte Task-Suche',
  stateLabel: 'Zustand',
  allStates: 'Alle Zustände',
  searchPlaceholder: 'Task-Name, Fehler oder JSON...',
  executeQuery: 'Abfrage ausführen',
  attempts: 'Versuche',
  enqueuedAt: 'Eingereiht',

  adminQueues: 'Queues',
  adminActions: 'Admin-Aktionen',
  adminOperation: 'Operation',
  createQueue: 'Queue erstellen',
  dropQueue: 'Queue löschen',
  newQueueName: 'Neuer Queue-Name',

  cancelTask: 'Task abbrechen',
  bulkCancelTasks: 'Tasks gesammelt abbrechen',
  releaseWorkerClaims: 'Worker-Claims freigeben',
  extendClaim: 'Claim verlängern',
  wakeRun: 'Run aufwecken',
  completeRun: 'Run manuell abschließen',
  failRun: 'Run manuell fehlschlagen lassen',
  emitEvent: 'Event senden',
  setCheckpoint: 'Checkpoint setzen',
  cleanupTasks: 'Tasks bereinigen',
  cleanupEvents: 'Events bereinigen',

  ownerRunId: 'Owner Run ID',
  stepName: 'Step Name',
  stateJson: 'State JSON',
  reasonJson: 'Reason JSON',
  payloadJson: 'Payload JSON',
  checkpointStateJson: 'Checkpoint State JSON',
  extendBySeconds: 'Verlängern um Sekunden',
  extendClaimBySeconds: 'Claim verlängern um Sekunden',
  wakeAt: 'Aufwecken um',
  retryAt: 'Retry um',
  ttlSeconds: 'TTL Sekunden',
  cleanupLimit: 'Cleanup-Limit',
  selectQueue: 'Queue auswählen',
  oneTaskIdPerLine: 'Eine Task ID pro Zeile oder kommasepariert',

  prev: 'Zurück',
  next: 'Weiter',
  page: 'Seite',
  pageSize: 'Seitengröße',
  serverLimit: 'Limit',
  window: 'Zeitraum',
  rows: 'Zeilen',
  loaded: 'geladen',
  visible: 'sichtbar',

  parametersJson: 'Parameter JSON',
  completedPayloadResult: 'Ergebnis-Payload JSON',

  noQueueData: 'Keine Daten vorhanden.',
  noActiveWorkers: 'Keine aktiven Worker vorhanden.',
  noWaitTimes: 'Keine Wartezeitdaten vorhanden.',
  noThroughput: 'Keine Durchsatzdaten vorhanden.',
  noLatencyData: 'Keine Latenzdaten vorhanden.',
  noHotspots: 'Keine Fehler-Hotspots vorhanden.',
  noFailedTaskList: 'Keine fehlgeschlagenen Tasks vorhanden.',
  noSlowTasks: 'Keine langsamen Tasks vorhanden.',
  noRetryHotspots: 'Keine Retry-Hotspots vorhanden.',
  noBlockades: 'Keine Event-Blockaden vorhanden.',
  noMatchingTasks: 'Keine passenden Tasks gefunden.',
  noAdminQueues: 'Keine Queues vorhanden.',

  deleteQueueConfirm: 'Diese Queue und alle zugehörigen Daten löschen?',
  releaseClaimsConfirm: 'Claims für diesen Worker freigeben?',
  cancelTaskConfirm: 'Diesen Task abbrechen?'
};

export const TRANSLATIONS_ZH: DashboardTranslations = {
  ...TRANSLATIONS_EN,
  title: 'SqlFlow 系统浏览器',
  subtitle: '企业级工作流遥测与管理控制台',
  autoRefresh: '自动刷新',
  refresh: '刷新',
  language: '语言',
  quickFilter: '快速筛选...',
  monitoring: '监控',
  analytics: '分析',
  operations: '操作',
  administration: '管理',
  backlog: '积压',
  activeExecutions: '运行中',
  completed: '已完成',
  failedTasks: '失败',
  dbStorage: '数据库大小',
  queue: '队列',
  state: '状态',
  count: '数量',
  queueStateDist: '队列状态',
  activeWorkers: '活跃工作节点',
  backlogDepth: '积压深度',
  waitTimes: '等待时间',
  throughput: '吞吐量',
  latencyPercentiles: '延迟百分位数',
  failureHotspots: '失败热点',
  failedTaskList: '失败任务',
  slowTasks: '慢任务',
  retryHotspots: '重试热点',
  eventBlockades: '事件阻塞',
  inspectorTab: '高级任务搜索',
  adminQueues: '队列',
  adminActions: '管理操作',
  createQueue: '创建队列',
  dropQueue: '删除队列',
  cancelTask: '取消任务',
  bulkCancelTasks: '批量取消任务',
  releaseWorkerClaims: '释放工作节点声明',
  extendClaim: '延长声明',
  wakeRun: '唤醒运行',
  completeRun: '强制完成运行',
  failRun: '强制失败运行',
  emitEvent: '发送事件',
  setCheckpoint: '设置检查点',
  cleanupTasks: '清理任务',
  cleanupEvents: '清理事件',
  prev: '上一页',
  next: '下一页',
  close: '关闭',
  execute: '执行'
};

/* ============================================================
   Service
   ============================================================ */

@Injectable({ providedIn: 'root' })
export class RelayDashboardService {
  private readonly http = inject(HttpClient);
  private readonly appSettings = inject(AppSettingsService);

  private get dashboardBaseUrl(): string {
    return this.appSettings.sqlFlowDashboardApiUrl;
  }

  private get adminBaseUrl(): string {
    return this.appSettings.sqlFlowAdminApiUrl;
  }

  readonly currentLanguage = signal<'en' | 'de' | 'zh'>('en');

  readonly t = computed(() => {
    switch (this.currentLanguage()) {
      case 'en': return TRANSLATIONS_EN;
      case 'zh': return TRANSLATIONS_ZH;
      default: return TRANSLATIONS_DE;
    }
  });

  readonly stats = signal<QueueStatItem[]>([]);
  readonly throughput = signal<ThroughputBucketItem[]>([]);
  readonly percentiles = signal<TaskPercentileItem[]>([]);
  readonly dbHealth = signal<DatabaseHealthItem | null>(null);
  readonly workers = signal<ActiveWorkerItem[]>([]);
  readonly waitTimes = signal<QueueWaitTimeItem[]>([]);
  readonly hotspots = signal<TaskFailureHotspotItem[]>([]);
  readonly activeWaits = signal<ActiveWaitItem[]>([]);
  readonly backlog = signal<QueueBacklogItem[]>([]);
  readonly retryHotspots = signal<RetryHotspotItem[]>([]);
  readonly slowTasks = signal<SlowTaskItem[]>([]);
  readonly failedTasks = signal<FailedTaskItem[]>([]);

  readonly searchResults = signal<TaskSearchResultItem[]>([]);
  readonly selectedTaskDetail = signal<TaskDetailItem | null>(null);

  readonly adminQueues = signal<string[]>([]);
  readonly adminMessage = signal<string | null>(null);

  readonly isLoading = signal(false);
  readonly isDetailLoading = signal(false);
  readonly isAdminLoading = signal(false);
  readonly errorMessage = signal<string | null>(null);
  readonly autoRefreshEnabled = signal(false);

  readonly fetchLimit = signal(50);
  readonly throughputWindowSeconds = signal(3600);

  readonly selectedQueue = signal('');
  readonly selectedState = signal('all');
  readonly searchTerm = signal('');

  readonly minAttempts = signal<number | null>(null);
  readonly maxAttempts = signal<number | null>(null);
  readonly claimedBy = signal('');
  readonly fromDate = signal('');
  readonly toDate = signal('');
  readonly sortBy = signal('enqueue_at');
  readonly sortDescending = signal(true);

  readonly page = signal(1);
  readonly pageSize = signal(50);

  readonly totalPending = computed(() =>
    this.stats().filter(x => x.state === 'pending').reduce((sum, x) => sum + x.count, 0)
  );

  readonly totalRunning = computed(() =>
    this.stats().filter(x => x.state === 'running').reduce((sum, x) => sum + x.count, 0)
  );

  readonly totalFailed = computed(() =>
    this.stats().filter(x => x.state === 'failed').reduce((sum, x) => sum + x.count, 0)
  );

  readonly totalCompleted = computed(() =>
    this.stats().filter(x => x.state === 'completed').reduce((sum, x) => sum + x.count, 0)
  );

  readonly availableQueues = computed(() => {
    const names = [...this.stats().map(x => x.queueName), ...this.adminQueues()];
    return Array.from(new Set(names)).sort();
  });

  private handleError(error: any) {
    this.isLoading.set(false);
    this.isDetailLoading.set(false);
    this.isAdminLoading.set(false);

    const msg = error?.error?.message || error?.message || 'API request failed.';
    this.errorMessage.set(msg);

    return of([] as any);
  }

  /* Query API */

  loadOverviewMetrics(): void {
    this.isLoading.set(true);
    this.errorMessage.set(null);

    forkJoin({
      stats: this.http.get<QueueStatItem[]>(`${this.dashboardBaseUrl}/stats`)
        .pipe(catchError(err => this.handleError(err))),
      workers: this.http.get<ActiveWorkerItem[]>(`${this.dashboardBaseUrl}/workers`)
        .pipe(catchError(err => this.handleError(err))),
      backlog: this.http.get<QueueBacklogItem[]>(`${this.dashboardBaseUrl}/backlog?limit=${this.fetchLimit()}`)
        .pipe(catchError(err => this.handleError(err)))
    }).subscribe(result => {
      this.stats.set(result.stats);
      this.workers.set(result.workers);
      this.backlog.set(result.backlog);

      if (!this.selectedQueue() && result.stats.length > 0) {
        this.selectedQueue.set(result.stats[0].queueName);
      }

      this.loadDatabaseHealthForSelectedQueue();
      this.isLoading.set(false);
    });
  }

  loadAnalytics(): void {
    this.isLoading.set(true);
    this.errorMessage.set(null);

    const limit = this.fetchLimit();
    const queueName = this.selectedQueue();

    const latencyUrl = queueName
      ? `${this.dashboardBaseUrl}/latency-percentiles?queueName=${encodeURIComponent(queueName)}`
      : `${this.dashboardBaseUrl}/latency-percentiles`;

    forkJoin({
      percentiles: this.http.get<TaskPercentileItem[]>(latencyUrl)
        .pipe(catchError(err => this.handleError(err))),
      hotspots: this.http.get<TaskFailureHotspotItem[]>(`${this.dashboardBaseUrl}/hotspots/failures?lookbackSeconds=86400&limit=${limit}`)
        .pipe(catchError(err => this.handleError(err))),
      retries: this.http.get<RetryHotspotItem[]>(`${this.dashboardBaseUrl}/hotspots/retries?limit=${limit}`)
        .pipe(catchError(err => this.handleError(err))),
      activeWaits: this.http.get<ActiveWaitItem[]>(`${this.dashboardBaseUrl}/active-waits?limit=${limit}`)
        .pipe(catchError(err => this.handleError(err)))
    }).subscribe(result => {
      this.percentiles.set(result.percentiles);
      this.hotspots.set(result.hotspots);
      this.retryHotspots.set(result.retries);
      this.activeWaits.set(result.activeWaits);
      this.isLoading.set(false);
    });
  }

  loadWaitTimes(): void {
    this.isLoading.set(true);
    this.http.get<QueueWaitTimeItem[]>(`${this.dashboardBaseUrl}/wait-times?limit=${this.fetchLimit()}`)
      .pipe(catchError(err => this.handleError(err)))
      .subscribe(result => {
        this.waitTimes.set(result);
        this.isLoading.set(false);
      });
  }

  loadThroughput(): void {
    this.isLoading.set(true);
    this.http.get<ThroughputBucketItem[]>(`${this.dashboardBaseUrl}/throughput?windowSeconds=${this.throughputWindowSeconds()}`)
      .pipe(catchError(err => this.handleError(err)))
      .subscribe(result => {
        this.throughput.set(result);
        this.isLoading.set(false);
      });
  }

  loadSlowTasks(): void {
    this.isLoading.set(true);
    this.http.get<SlowTaskItem[]>(`${this.dashboardBaseUrl}/slow-tasks?limit=${this.fetchLimit()}`)
      .pipe(catchError(err => this.handleError(err)))
      .subscribe(result => {
        this.slowTasks.set(result);
        this.isLoading.set(false);
      });
  }

  loadFailedTasks(): void {
    this.isLoading.set(true);
    this.http.get<FailedTaskItem[]>(`${this.dashboardBaseUrl}/failed-tasks?limit=${this.fetchLimit()}`)
      .pipe(catchError(err => this.handleError(err)))
      .subscribe(result => {
        this.failedTasks.set(result);
        this.isLoading.set(false);
      });
  }

  loadDatabaseHealthForSelectedQueue(): void {
    const queueName = this.selectedQueue();

    if (!queueName) {
      this.dbHealth.set(null);
      return;
    }

    this.http.get<DatabaseHealthItem>(
      `${this.dashboardBaseUrl}/health?queueName=${encodeURIComponent(queueName)}`
    )
      .pipe(catchError(() => {
        this.dbHealth.set(null);
        return EMPTY;
      }))
      .subscribe(result => this.dbHealth.set(result));
  }

  searchTasks(): void {
    if (!this.selectedQueue()) {
      this.searchResults.set([]);
      return;
    }

    this.isLoading.set(true);

    const filter: TaskSearchFilter = {
      queueName: this.selectedQueue(),
      states: this.selectedState() !== 'all' ? [this.selectedState()] : undefined,
      searchTerm: this.searchTerm() || undefined,
      minAttempts: this.minAttempts() ?? undefined,
      maxAttempts: this.maxAttempts() ?? undefined,
      claimedBy: this.claimedBy() || undefined,
      fromDate: this.fromDate() ? new Date(this.fromDate()).toISOString() : undefined,
      toDate: this.toDate() ? new Date(this.toDate()).toISOString() : undefined,
      sortBy: this.sortBy(),
      sortDescending: this.sortDescending(),
      offset: (this.page() - 1) * this.pageSize(),
      limit: this.pageSize()
    };

    this.http.post<TaskSearchResultItem[]>(`${this.dashboardBaseUrl}/tasks/search`, filter)
      .pipe(catchError(err => this.handleError(err)))
      .subscribe(result => {
        this.searchResults.set(result);
        this.isLoading.set(false);
      });
  }

  fetchTaskDetails(queueName: string, taskId: string): void {
    this.isDetailLoading.set(true);

    this.http.get<TaskDetailItem>(
      `${this.dashboardBaseUrl}/tasks/${encodeURIComponent(queueName)}/${encodeURIComponent(taskId)}`
    )
      .pipe(catchError(() => {
        this.isDetailLoading.set(false);
        this.errorMessage.set(`Task details failed for ${taskId}`);
        return EMPTY;
      }))
      .subscribe(result => {
        this.selectedTaskDetail.set(result);
        this.isDetailLoading.set(false);
      });
  }

  nextSearchPage(): void {
    this.page.update(x => x + 1);
    this.searchTasks();
  }

  previousSearchPage(): void {
    this.page.update(x => Math.max(1, x - 1));
    this.searchTasks();
  }

  /* Admin API */

  loadAdminQueues(): void {
    this.isAdminLoading.set(true);

    this.http.get<string[]>(`${this.adminBaseUrl}/queues`)
      .pipe(catchError(err => this.handleError(err)))
      .subscribe(result => {
        this.adminQueues.set(result);
        this.isAdminLoading.set(false);
      });
  }

  createQueue(command: CreateQueueCommand): void {
    this.adminPost('/queues', {
      queueName: command.queueName,
      storageMode: command.storageMode ?? 'unpartitioned'
    }, () => {
      this.loadAdminQueues();
      this.loadOverviewMetrics();
    });
  }

  dropQueue(queueName: string): void {
    this.isAdminLoading.set(true);
    this.adminMessage.set(null);

    this.http.delete<AdminResponse>(`${this.adminBaseUrl}/queues/${encodeURIComponent(queueName)}`)
      .pipe(catchError(err => this.handleError(err)))
      .subscribe(result => {
        this.adminMessage.set(result.message ?? `Queue '${queueName}' dropped.`);
        this.isAdminLoading.set(false);
        this.loadAdminQueues();
        this.loadOverviewMetrics();
      });
  }

  cancelTask(command: CancelTaskCommand): void {
    this.adminPost('/tasks/cancel', command, () => this.refreshAfterAdminTaskChange());
  }

  bulkCancelTasks(command: BulkCancelTasksCommand): void {
    this.adminPost('/tasks/bulk-cancel', command, () => this.refreshAfterAdminTaskChange());
  }

  releaseWorkerClaims(command: ReleaseWorkerClaimsCommand): void {
    this.adminPost('/workers/release-claims', command, () => this.loadOverviewMetrics());
  }

  extendClaim(command: ExtendClaimCommand): void {
    this.adminPost('/runs/extend-claim', {
      queueName: command.queueName,
      runId: command.runId,
      extendBySeconds: command.extendBySeconds ?? 30
    }, () => this.refreshAfterAdminTaskChange());
  }

  wakeRun(command: ScheduleWakeupCommand): void {
    this.adminPost('/runs/wake', command, () => this.refreshAfterAdminTaskChange());
  }

  completeRun(command: CompleteRunCommand): void {
    this.adminPost('/runs/complete', command, () => this.refreshAfterAdminTaskChange());
  }

  failRun(command: FailRunCommand): void {
    this.adminPost('/runs/fail', command, () => this.refreshAfterAdminTaskChange());
  }

  emitEvent(command: EmitEventCommand): void {
    this.adminPost('/events/emit', command, () => this.loadAnalytics());
  }

  setCheckpoint(command: SetCheckpointCommand): void {
    this.adminPost('/checkpoints', command, () => this.refreshAfterAdminTaskChange());
  }

  cleanupTasks(command: CleanupCommand): void {
    this.adminPost('/cleanup/tasks', {
      queueName: command.queueName,
      ttlSeconds: command.ttlSeconds,
      limit: command.limit ?? 1000
    }, () => this.refreshAfterAdminTaskChange());
  }

  cleanupEvents(command: CleanupCommand): void {
    this.adminPost('/cleanup/events', {
      queueName: command.queueName,
      ttlSeconds: command.ttlSeconds,
      limit: command.limit ?? 1000
    }, () => this.loadAnalytics());
  }

  private adminPost<T>(path: string, command: T, afterSuccess?: () => void): void {
    this.isAdminLoading.set(true);
    this.adminMessage.set(null);
    this.errorMessage.set(null);

    this.http.post<AdminResponse>(`${this.adminBaseUrl}${path}`, command)
      .pipe(catchError(err => this.handleError(err)))
      .subscribe(result => {
        if (result.deletedCount !== undefined) {
          this.adminMessage.set(result.message ?? `Deleted ${result.deletedCount} records.`);
        } else {
          this.adminMessage.set(result.message ?? 'Admin operation completed.');
        }

        this.isAdminLoading.set(false);
        afterSuccess?.();
      });
  }

  private refreshAfterAdminTaskChange(): void {
    this.loadOverviewMetrics();
    this.loadAnalytics();
    this.loadFailedTasks();
    this.loadSlowTasks();

    if (this.selectedQueue()) {
      this.searchTasks();
    }
  }
}

/* ============================================================
   Component
   ============================================================ */

type ViewId =
  | 'queue-states'
  | 'workers'
  | 'backlog'
  | 'wait-times'
  | 'throughput'
  | 'latency'
  | 'failures'
  | 'failed-tasks'
  | 'slow-tasks'
  | 'retries'
  | 'blockades'
  | 'search'
  | 'admin-queues'
  | 'admin-actions';

type ColumnKind = 'text' | 'number' | 'badge' | 'date' | 'action';

interface EnterpriseColumn {
  key: string;
  label: string;
  kind?: ColumnKind;
  align?: 'left' | 'right' | 'center';
  value: (row: any) => string | number | null | undefined;
}

type AdminOperation =
  | 'cancel-task'
  | 'bulk-cancel'
  | 'release-worker-claims'
  | 'extend-claim'
  | 'wake-run'
  | 'complete-run'
  | 'fail-run'
  | 'emit-event'
  | 'set-checkpoint'
  | 'cleanup-tasks'
  | 'cleanup-events';

@Component({
  selector: 'app-root',
  imports: [CommonModule, FormsModule],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="h-screen w-full flex flex-col bg-slate-100 text-slate-900 font-sans overflow-hidden text-sm">

      <header class="flex-none flex items-center justify-between px-6 py-4 border-b border-slate-300 bg-white">
        <div class="flex items-center gap-4">
          <div class="h-9 w-9 rounded-lg bg-slate-900 flex items-center justify-center font-bold text-base text-white">
            ⚡
          </div>

          <div>
            <h1 class="text-xl font-bold text-slate-950">{{ service.t().title }}</h1>
            <p class="text-sm text-slate-500 hidden md:block">{{ service.t().subtitle }}</p>
          </div>
        </div>

        <div class="flex items-center gap-5">
          @if (service.errorMessage()) {
            <span class="text-rose-600 font-bold truncate max-w-md" [title]="service.errorMessage()">
              ⚠️ {{ service.errorMessage() }}
            </span>
          }

          @if (service.adminMessage()) {
            <span class="text-emerald-700 font-bold truncate max-w-md" [title]="service.adminMessage()">
              ✅ {{ service.adminMessage() }}
            </span>
          }

          <div class="flex items-center gap-2">
            <span class="text-slate-600 font-semibold">{{ service.t().language }}:</span>
            <select
              [ngModel]="service.currentLanguage()"
              (ngModelChange)="service.currentLanguage.set($event)"
              class="bg-white border border-slate-300 rounded-md px-2 py-1 font-semibold">
              <option value="de">DE</option>
              <option value="en">EN</option>
              <option value="zh">ZH</option>
            </select>
          </div>

          <label class="flex items-center gap-2 text-slate-700 cursor-pointer font-semibold">
            <input
              type="checkbox"
              [ngModel]="service.autoRefreshEnabled()"
              (ngModelChange)="service.autoRefreshEnabled.set($event)"
              class="rounded w-4 h-4" />
            {{ service.t().autoRefresh }}
          </label>

          <button
            (click)="refreshCurrentView()"
            [disabled]="service.isLoading() || service.isAdminLoading()"
            class="bg-slate-900 hover:bg-slate-800 disabled:opacity-50 text-white px-4 py-2 rounded-md font-bold">
            {{ service.t().refresh }}
          </button>
        </div>
      </header>

      <div class="flex-none flex items-center gap-8 px-6 py-3 bg-white border-b border-slate-300 overflow-x-auto shadow-sm">
        <button (click)="selectView('backlog')" class="flex items-center gap-2 hover:bg-slate-100 px-2 py-1 rounded-md">
          <span class="font-bold text-slate-500 uppercase tracking-wider">{{ service.t().backlog }}:</span>
          <span class="font-mono font-bold text-amber-600 text-base">{{ service.totalPending() | number }}</span>
        </button>

        <div class="w-px h-5 bg-slate-300"></div>

        <button (click)="selectView('workers')" class="flex items-center gap-2 hover:bg-slate-100 px-2 py-1 rounded-md">
          <span class="font-bold text-slate-500 uppercase tracking-wider">{{ service.t().activeExecutions }}:</span>
          <span class="font-mono font-bold text-sky-600 text-base">{{ service.totalRunning() | number }}</span>
        </button>

        <div class="w-px h-5 bg-slate-300"></div>

        <button (click)="selectView('queue-states')" class="flex items-center gap-2 hover:bg-slate-100 px-2 py-1 rounded-md">
          <span class="font-bold text-slate-500 uppercase tracking-wider">{{ service.t().completed }}:</span>
          <span class="font-mono font-bold text-emerald-600 text-base">{{ service.totalCompleted() | number }}</span>
        </button>

        <div class="w-px h-5 bg-slate-300"></div>

        <button (click)="selectView('failed-tasks')" class="flex items-center gap-2 hover:bg-slate-100 px-2 py-1 rounded-md">
          <span class="font-bold text-slate-500 uppercase tracking-wider">{{ service.t().failedTasks }}:</span>
          <span class="font-mono font-bold text-rose-600 text-base">{{ service.totalFailed() | number }}</span>
        </button>

        <div class="w-px h-5 bg-slate-300"></div>

        <div class="flex items-center gap-2">
          <span class="font-bold text-slate-500 uppercase tracking-wider">{{ service.t().dbStorage }}:</span>
          <span class="font-mono font-bold text-slate-800 text-base">
            {{
              service.dbHealth()
                ? ((service.dbHealth()!.tasksTableBytes + service.dbHealth()!.runsTableBytes) / 1024 / 1024 | number:'1.1-1') + ' MB'
                : '-'
            }}
          </span>
        </div>
      </div>

      <main class="flex-1 flex overflow-hidden">

        <aside class="w-72 bg-white border-r border-slate-300 overflow-auto flex-none">
          @for (group of navigation(); track group.label) {
            <div class="p-4 border-b border-slate-100">
              <div class="text-xs font-bold uppercase tracking-wider text-slate-500 mb-3">
                {{ group.label }}
              </div>

              <div class="flex flex-col gap-1">
                @for (item of group.children; track item.id) {
                  <button
                    (click)="selectView(item.id)"
                    class="w-full flex items-center justify-between text-left px-3 py-2 rounded-md font-semibold transition border"
                    [class.bg-slate-900]="selectedView() === item.id"
                    [class.text-white]="selectedView() === item.id"
                    [class.border-slate-900]="selectedView() === item.id"
                    [class.text-slate-700]="selectedView() !== item.id"
                    [class.border-transparent]="selectedView() !== item.id"
                    [class.hover:bg-slate-100]="selectedView() !== item.id">
                    <span>{{ item.label }}</span>
                    @if (selectedView() === item.id) {
                      <span class="text-xs opacity-80">●</span>
                    }
                  </button>
                }
              </div>
            </div>
          }
        </aside>

        <section class="flex-1 overflow-hidden flex flex-col">

          <div class="flex-none bg-white border-b border-slate-300 px-6 py-4">
            <div class="flex items-center justify-between gap-4">
              <div>
                <h2 class="text-lg font-bold text-slate-950">{{ currentTitle() }}</h2>
                <p class="text-sm text-slate-500">{{ totalRows() | number }} {{ service.t().rows }}</p>
              </div>

              <div class="flex items-center gap-3">

                @if (selectedView() === 'search') {
                  <select
                    [ngModel]="service.selectedQueue()"
                    (ngModelChange)="onSearchQueueChange($event)"
                    class="h-10 px-3 text-sm border border-slate-300 rounded-md bg-slate-50">
                    <option value="">{{ service.t().queueLabel }}</option>
                    @for (queue of service.availableQueues(); track queue) {
                      <option [value]="queue">{{ queue }}</option>
                    }
                  </select>

                  <select
                    [ngModel]="service.selectedState()"
                    (ngModelChange)="onSearchStateChange($event)"
                    class="h-10 px-3 text-sm border border-slate-300 rounded-md bg-slate-50">
                    <option value="all">{{ service.t().allStates }}</option>
                    <option value="pending">pending</option>
                    <option value="running">running</option>
                    <option value="completed">completed</option>
                    <option value="failed">failed</option>
                    <option value="sleeping">sleeping</option>
                  </select>
                }

                @if (selectedView() !== 'admin-actions') {
                  <input
                    type="text"
                    [ngModel]="clientFilter()"
                    (ngModelChange)="onFilterChange($event)"
                    [placeholder]="selectedView() === 'search' ? service.t().searchPlaceholder : service.t().quickFilter"
                    class="h-10 px-3 text-sm border border-slate-300 rounded-md bg-slate-50 w-72 outline-none focus:ring-2 focus:ring-slate-400" />
                }

                @if (selectedView() === 'search') {
                  <select
                    [ngModel]="service.pageSize()"
                    (ngModelChange)="onSearchPageSizeChange($event)"
                    class="h-10 px-2 text-sm border border-slate-300 rounded-md bg-slate-50">
                    <option [ngValue]="10">10</option>
                    <option [ngValue]="25">25</option>
                    <option [ngValue]="50">50</option>
                    <option [ngValue]="100">100</option>
                  </select>
                } @else if (selectedView() === 'throughput') {
                  <select
                    [ngModel]="service.throughputWindowSeconds()"
                    (ngModelChange)="onThroughputWindowChange($event)"
                    class="h-10 px-2 text-sm border border-slate-300 rounded-md bg-slate-50">
                    <option [ngValue]="900">15 min</option>
                    <option [ngValue]="1800">30 min</option>
                    <option [ngValue]="3600">1 h</option>
                    <option [ngValue]="21600">6 h</option>
                    <option [ngValue]="86400">24 h</option>
                  </select>
                } @else if (selectedView() !== 'admin-actions') {
                  <select
                    [ngModel]="service.fetchLimit()"
                    (ngModelChange)="onFetchLimitChange($event)"
                    class="h-10 px-2 text-sm border border-slate-300 rounded-md bg-slate-50">
                    <option [ngValue]="25">25</option>
                    <option [ngValue]="50">50</option>
                    <option [ngValue]="100">100</option>
                    <option [ngValue]="500">500</option>
                    <option [ngValue]="1000">1000</option>
                  </select>
                }

                @if (selectedView() !== 'admin-actions') {
                  <button
                    (click)="refreshCurrentView()"
                    [disabled]="service.isLoading() || service.isAdminLoading()"
                    class="h-10 px-4 bg-slate-900 hover:bg-slate-800 disabled:opacity-50 text-white rounded-md font-bold">
                    {{ service.t().refresh }}
                  </button>
                }
              </div>
            </div>
          </div>

          @if (selectedView() === 'admin-actions') {
            <div class="flex-1 overflow-auto p-6">
              <div class="bg-white border border-slate-300 rounded-lg shadow-sm p-6 max-w-5xl">
                <h3 class="text-lg font-bold text-slate-950 mb-4">{{ service.t().adminActions }}</h3>

                <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
                  <label class="flex flex-col gap-1">
                    <span class="font-semibold text-slate-600">{{ service.t().adminOperation }}</span>
                    <select
                      [ngModel]="adminOperation()"
                      (ngModelChange)="adminOperation.set($event)"
                      class="h-10 px-3 border border-slate-300 rounded-md bg-slate-50">
                      <option value="cancel-task">{{ service.t().cancelTask }}</option>
                      <option value="bulk-cancel">{{ service.t().bulkCancelTasks }}</option>
                      <option value="release-worker-claims">{{ service.t().releaseWorkerClaims }}</option>
                      <option value="extend-claim">{{ service.t().extendClaim }}</option>
                      <option value="wake-run">{{ service.t().wakeRun }}</option>
                      <option value="complete-run">{{ service.t().completeRun }}</option>
                      <option value="fail-run">{{ service.t().failRun }}</option>
                      <option value="emit-event">{{ service.t().emitEvent }}</option>
                      <option value="set-checkpoint">{{ service.t().setCheckpoint }}</option>
                      <option value="cleanup-tasks">{{ service.t().cleanupTasks }}</option>
                      <option value="cleanup-events">{{ service.t().cleanupEvents }}</option>
                    </select>
                  </label>

                  <label class="flex flex-col gap-1">
                    <span class="font-semibold text-slate-600">{{ service.t().queue }}</span>
                    <select
                      [ngModel]="adminQueueName()"
                      (ngModelChange)="adminQueueName.set($event)"
                      class="h-10 px-3 border border-slate-300 rounded-md bg-slate-50">
                      <option value="">{{ service.t().selectQueue }}</option>
                      @for (queue of service.availableQueues(); track queue) {
                        <option [value]="queue">{{ queue }}</option>
                      }
                    </select>
                  </label>
                </div>

                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">

                  @if (requiresTaskId()) {
                    <label class="flex flex-col gap-1">
                      <span class="font-semibold text-slate-600">{{ service.t().taskId }}</span>
                      <input
                        [ngModel]="adminTaskId()"
                        (ngModelChange)="adminTaskId.set($event)"
                        class="h-10 px-3 border border-slate-300 rounded-md bg-slate-50" />
                    </label>
                  }

                  @if (adminOperation() === 'bulk-cancel') {
                    <label class="flex flex-col gap-1 md:col-span-2">
                      <span class="font-semibold text-slate-600">{{ service.t().taskIds }}</span>
                      <textarea
                        [ngModel]="adminTaskIdsText()"
                        (ngModelChange)="adminTaskIdsText.set($event)"
                        [placeholder]="service.t().oneTaskIdPerLine"
                        class="min-h-32 px-3 py-2 border border-slate-300 rounded-md bg-slate-50"></textarea>
                    </label>
                  }

                  @if (requiresRunId()) {
                    <label class="flex flex-col gap-1">
                      <span class="font-semibold text-slate-600">{{ service.t().runId }}</span>
                      <input
                        [ngModel]="adminRunId()"
                        (ngModelChange)="adminRunId.set($event)"
                        class="h-10 px-3 border border-slate-300 rounded-md bg-slate-50" />
                    </label>
                  }

                  @if (adminOperation() === 'release-worker-claims') {
                    <label class="flex flex-col gap-1">
                      <span class="font-semibold text-slate-600">{{ service.t().workerId }}</span>
                      <input
                        [ngModel]="adminWorkerId()"
                        (ngModelChange)="adminWorkerId.set($event)"
                        class="h-10 px-3 border border-slate-300 rounded-md bg-slate-50" />
                    </label>
                  }

                  @if (adminOperation() === 'extend-claim') {
                    <label class="flex flex-col gap-1">
                      <span class="font-semibold text-slate-600">{{ service.t().extendBySeconds }}</span>
                      <input
                        type="number"
                        [ngModel]="adminExtendBySeconds()"
                        (ngModelChange)="adminExtendBySeconds.set($event)"
                        class="h-10 px-3 border border-slate-300 rounded-md bg-slate-50" />
                    </label>
                  }

                  @if (adminOperation() === 'wake-run') {
                    <label class="flex flex-col gap-1">
                      <span class="font-semibold text-slate-600">{{ service.t().wakeAt }}</span>
                      <input
                        type="datetime-local"
                        [ngModel]="adminWakeAt()"
                        (ngModelChange)="adminWakeAt.set($event)"
                        class="h-10 px-3 border border-slate-300 rounded-md bg-slate-50" />
                    </label>
                  }

                  @if (adminOperation() === 'fail-run') {
                    <label class="flex flex-col gap-1 md:col-span-2">
                      <span class="font-semibold text-slate-600">{{ service.t().reasonJson }}</span>
                      <textarea
                        [ngModel]="adminReasonJson()"
                        (ngModelChange)="adminReasonJson.set($event)"
                        class="min-h-24 px-3 py-2 border border-slate-300 rounded-md bg-slate-50"></textarea>
                    </label>

                    <label class="flex flex-col gap-1">
                      <span class="font-semibold text-slate-600">{{ service.t().retryAt }}</span>
                      <input
                        type="datetime-local"
                        [ngModel]="adminRetryAt()"
                        (ngModelChange)="adminRetryAt.set($event)"
                        class="h-10 px-3 border border-slate-300 rounded-md bg-slate-50" />
                    </label>
                  }

                  @if (adminOperation() === 'complete-run') {
                    <label class="flex flex-col gap-1 md:col-span-2">
                      <span class="font-semibold text-slate-600">{{ service.t().stateJson }}</span>
                      <textarea
                        [ngModel]="adminStateJson()"
                        (ngModelChange)="adminStateJson.set($event)"
                        class="min-h-24 px-3 py-2 border border-slate-300 rounded-md bg-slate-50"></textarea>
                    </label>
                  }

                  @if (adminOperation() === 'emit-event') {
                    <label class="flex flex-col gap-1">
                      <span class="font-semibold text-slate-600">{{ service.t().eventName }}</span>
                      <input
                        [ngModel]="adminEventName()"
                        (ngModelChange)="adminEventName.set($event)"
                        class="h-10 px-3 border border-slate-300 rounded-md bg-slate-50" />
                    </label>

                    <label class="flex flex-col gap-1 md:col-span-2">
                      <span class="font-semibold text-slate-600">{{ service.t().payloadJson }}</span>
                      <textarea
                        [ngModel]="adminPayloadJson()"
                        (ngModelChange)="adminPayloadJson.set($event)"
                        class="min-h-24 px-3 py-2 border border-slate-300 rounded-md bg-slate-50"></textarea>
                    </label>
                  }

                  @if (adminOperation() === 'set-checkpoint') {
                    <label class="flex flex-col gap-1">
                      <span class="font-semibold text-slate-600">{{ service.t().ownerRunId }}</span>
                      <input
                        [ngModel]="adminOwnerRunId()"
                        (ngModelChange)="adminOwnerRunId.set($event)"
                        class="h-10 px-3 border border-slate-300 rounded-md bg-slate-50" />
                    </label>

                    <label class="flex flex-col gap-1">
                      <span class="font-semibold text-slate-600">{{ service.t().stepName }}</span>
                      <input
                        [ngModel]="adminStepName()"
                        (ngModelChange)="adminStepName.set($event)"
                        class="h-10 px-3 border border-slate-300 rounded-md bg-slate-50" />
                    </label>

                    <label class="flex flex-col gap-1">
                      <span class="font-semibold text-slate-600">{{ service.t().extendClaimBySeconds }}</span>
                      <input
                        type="number"
                        [ngModel]="adminExtendCheckpointBySeconds()"
                        (ngModelChange)="adminExtendCheckpointBySeconds.set($event)"
                        class="h-10 px-3 border border-slate-300 rounded-md bg-slate-50" />
                    </label>

                    <label class="flex flex-col gap-1 md:col-span-2">
                      <span class="font-semibold text-slate-600">{{ service.t().checkpointStateJson }}</span>
                      <textarea
                        [ngModel]="adminCheckpointStateJson()"
                        (ngModelChange)="adminCheckpointStateJson.set($event)"
                        class="min-h-24 px-3 py-2 border border-slate-300 rounded-md bg-slate-50"></textarea>
                    </label>
                  }

                  @if (adminOperation() === 'cleanup-tasks' || adminOperation() === 'cleanup-events') {
                    <label class="flex flex-col gap-1">
                      <span class="font-semibold text-slate-600">{{ service.t().ttlSeconds }}</span>
                      <input
                        type="number"
                        [ngModel]="adminTtlSeconds()"
                        (ngModelChange)="adminTtlSeconds.set($event)"
                        class="h-10 px-3 border border-slate-300 rounded-md bg-slate-50" />
                    </label>

                    <label class="flex flex-col gap-1">
                      <span class="font-semibold text-slate-600">{{ service.t().cleanupLimit }}</span>
                      <input
                        type="number"
                        [ngModel]="adminCleanupLimit()"
                        (ngModelChange)="adminCleanupLimit.set($event)"
                        class="h-10 px-3 border border-slate-300 rounded-md bg-slate-50" />
                    </label>
                  }
                </div>

                <div class="mt-6 flex justify-end">
                  <button
                    (click)="executeAdminOperation()"
                    [disabled]="!canExecuteAdminOperation() || service.isAdminLoading()"
                    class="px-5 py-2 bg-slate-900 hover:bg-slate-800 disabled:opacity-50 text-white rounded-md font-bold">
                    {{ service.t().execute }}
                  </button>
                </div>
              </div>
            </div>
          } @else {
            <div class="flex-1 overflow-auto p-6">
              <div class="bg-white border border-slate-300 shadow-sm rounded-lg overflow-hidden flex flex-col min-h-full">

                @if (selectedView() === 'admin-queues') {
                  <div class="flex-none p-4 border-b border-slate-200 bg-slate-50 flex gap-3">
                    <input
                      [ngModel]="newQueueName()"
                      (ngModelChange)="newQueueName.set($event)"
                      [placeholder]="service.t().newQueueName"
                      class="h-10 px-3 border border-slate-300 rounded-md bg-white w-64" />

                    <select
                      [ngModel]="newQueueStorageMode()"
                      (ngModelChange)="newQueueStorageMode.set($event)"
                      class="h-10 px-3 border border-slate-300 rounded-md bg-white">
                      <option value="unpartitioned">{{ service.t().unpartitioned }}</option>
                      <option value="partitioned">{{ service.t().partitioned }}</option>
                    </select>

                    <button
                      (click)="createQueueFromToolbar()"
                      [disabled]="!newQueueName().trim() || service.isAdminLoading()"
                      class="h-10 px-4 bg-emerald-700 hover:bg-emerald-800 disabled:opacity-50 text-white rounded-md font-bold">
                      {{ service.t().createQueue }}
                    </button>
                  </div>
                }

                <div class="overflow-auto flex-1">
                  <table class="w-full text-left text-sm whitespace-nowrap">
                    <thead class="bg-slate-50 text-slate-600 sticky top-0 border-b-2 border-slate-200 shadow-sm z-10">
                      <tr>
                        @for (col of currentColumns(); track col.key) {
                          <th
                            class="px-4 py-3 font-bold uppercase tracking-wider text-xs"
                            [class.text-right]="col.align === 'right'"
                            [class.text-center]="col.align === 'center'">
                            {{ col.label }}
                          </th>
                        }
                      </tr>
                    </thead>

                    <tbody class="divide-y divide-slate-100">
                      @for (row of currentRows(); track $index) {
                        <tr class="hover:bg-slate-50 transition-colors">
                          @for (col of currentColumns(); track col.key) {
                            <td
                              class="px-4 py-3 align-top"
                              [class.text-right]="col.align === 'right'"
                              [class.text-center]="col.align === 'center'">

                              @switch (col.kind || 'text') {
                                @case ('badge') {
                                  <span
                                    class="inline-flex items-center px-2 py-0.5 rounded border font-bold text-xs"
                                    [ngClass]="badgeClass(cell(row, col))">
                                    {{ cell(row, col) }}
                                  </span>
                                }

                                @case ('number') {
                                  <span class="font-mono font-bold text-slate-800">
                                    {{ cell(row, col) | number }}
                                  </span>
                                }

                                @case ('date') {
                                  <span class="font-mono text-slate-600">
                                    {{ cell(row, col) || '-' }}
                                  </span>
                                }

                                @case ('action') {
                                  <button
                                    (click)="onRowAction(row)"
                                    class="px-3 py-1.5 rounded-md bg-slate-900 hover:bg-slate-800 text-white font-bold text-xs shadow-sm">
                                    {{ cell(row, col) }}
                                  </button>
                                }

                                @default {
                                  <span class="block truncate max-w-[640px]" [title]="cell(row, col)">
                                    {{ cell(row, col) || '-' }}
                                  </span>
                                }
                              }
                            </td>
                          }
                        </tr>
                      } @empty {
                        <tr>
                          <td
                            [attr.colspan]="currentColumns().length"
                            class="px-4 py-12 text-center text-slate-500 font-semibold">
                            {{ emptyMessage() }}
                          </td>
                        </tr>
                      }
                    </tbody>
                  </table>
                </div>

                <div class="flex-none bg-slate-50 border-t border-slate-200 px-4 py-3 flex justify-between items-center text-sm">
                  <div class="text-slate-500 font-semibold">
                    @if (selectedView() === 'search') {
                      {{ service.t().page }} {{ service.page() }}
                      <span class="text-slate-400">· {{ service.searchResults().length | number }} {{ service.t().loaded }}</span>
                    } @else if (selectedView() === 'throughput') {
                      {{ service.t().window }} {{ service.throughputWindowSeconds() }}s
                      <span class="text-slate-400">· {{ currentRows().length | number }} {{ service.t().visible }}</span>
                    } @else {
                      {{ service.t().serverLimit }} {{ service.fetchLimit() }}
                      <span class="text-slate-400">· {{ currentRows().length | number }} {{ service.t().visible }}</span>
                    }
                  </div>

                  @if (selectedView() === 'search') {
                    <div class="flex gap-2">
                      <button
                        (click)="previousPage()"
                        [disabled]="service.page() === 1"
                        class="px-3 py-1.5 bg-white border border-slate-300 rounded-md font-semibold disabled:opacity-50 hover:bg-slate-100">
                        {{ service.t().prev }}
                      </button>

                      <button
                        (click)="nextPage()"
                        [disabled]="service.searchResults().length < service.pageSize()"
                        class="px-3 py-1.5 bg-white border border-slate-300 rounded-md font-semibold disabled:opacity-50 hover:bg-slate-100">
                        {{ service.t().next }}
                      </button>
                    </div>
                  }
                </div>
              </div>
            </div>
          }
        </section>
      </main>

      @if (service.selectedTaskDetail()) {
        <div class="fixed inset-0 bg-slate-900/60 flex items-center justify-center p-6 z-50">
          <div class="bg-white border border-slate-300 shadow-2xl w-full max-w-5xl flex flex-col max-h-[90vh] rounded-lg overflow-hidden">

            <div class="flex items-center justify-between border-b border-slate-200 p-5 bg-slate-50">
              <div>
                <h3 class="text-lg font-bold text-slate-900">
                  {{ service.selectedTaskDetail()!.taskName }}
                </h3>

                <p class="text-sm font-mono text-slate-600 mt-1">
                  {{ service.selectedTaskDetail()!.taskId }}
                  |
                  {{ service.t().queue }}: {{ service.selectedTaskDetail()!.queueName }}
                </p>
              </div>

              <div class="flex items-center gap-2">
                <button
                  (click)="cancelSelectedTask()"
                  class="px-3 py-1.5 rounded-md bg-rose-700 hover:bg-rose-800 text-white font-bold text-xs">
                  {{ service.t().cancelTask }}
                </button>

                <button
                  (click)="service.selectedTaskDetail.set(null)"
                  class="text-slate-400 hover:text-slate-900 text-2xl font-bold">
                  ✕
                </button>
              </div>
            </div>

            <div class="flex-1 overflow-auto p-6 space-y-6 bg-slate-950">
              <div>
                <div class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">
                  {{ service.t().parametersJson }}
                </div>
                <pre class="text-emerald-400 font-mono text-sm whitespace-pre-wrap leading-relaxed">{{ service.selectedTaskDetail()!.params || 'null' }}</pre>
              </div>

              <div class="border-t border-slate-800 pt-6">
                <div class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">
                  {{ service.t().completedPayloadResult }}
                </div>
                <pre class="text-sky-400 font-mono text-sm whitespace-pre-wrap leading-relaxed">{{ service.selectedTaskDetail()!.completedPayload || 'null' }}</pre>
              </div>
            </div>

            <div class="p-4 border-t border-slate-200 bg-slate-50 text-right">
              <button
                (click)="service.selectedTaskDetail.set(null)"
                class="bg-slate-200 hover:bg-slate-300 text-slate-900 px-6 py-2 rounded-md font-bold border border-slate-300">
                {{ service.t().close }}
              </button>
            </div>
          </div>
        </div>
      }
    </div>
  `
})
export class App implements OnInit, OnDestroy {
  readonly service = inject(RelayDashboardService);

  readonly selectedView = signal<ViewId>('queue-states');
  readonly clientFilter = signal('');

  readonly newQueueName = signal('');
  readonly newQueueStorageMode = signal('unpartitioned');

  readonly adminOperation = signal<AdminOperation>('cancel-task');
  readonly adminQueueName = signal('');
  readonly adminTaskId = signal('');
  readonly adminTaskIdsText = signal('');
  readonly adminRunId = signal('');
  readonly adminWorkerId = signal('');
  readonly adminExtendBySeconds = signal(30);
  readonly adminWakeAt = signal('');
  readonly adminRetryAt = signal('');
  readonly adminReasonJson = signal('{ "reason": "manual failure" }');
  readonly adminStateJson = signal('{}');
  readonly adminEventName = signal('');
  readonly adminPayloadJson = signal('');
  readonly adminOwnerRunId = signal('');
  readonly adminStepName = signal('');
  readonly adminCheckpointStateJson = signal('{}');
  readonly adminExtendCheckpointBySeconds = signal<number | null>(null);
  readonly adminTtlSeconds = signal(86400);
  readonly adminCleanupLimit = signal(1000);

  private destroy$ = new Subject<void>();

  readonly navigation = computed(() => {
    const t = this.service.t();

    return [
      {
        label: t.monitoring,
        children: [
          { id: 'queue-states' as ViewId, label: t.queueStateDist },
          { id: 'workers' as ViewId, label: t.activeWorkers },
          { id: 'backlog' as ViewId, label: t.backlogDepth },
          { id: 'wait-times' as ViewId, label: t.waitTimes }
        ]
      },
      {
        label: t.analytics,
        children: [
          { id: 'throughput' as ViewId, label: t.throughput },
          { id: 'latency' as ViewId, label: t.latencyPercentiles },
          { id: 'failures' as ViewId, label: t.failureHotspots },
          { id: 'failed-tasks' as ViewId, label: t.failedTaskList },
          { id: 'slow-tasks' as ViewId, label: t.slowTasks },
          { id: 'retries' as ViewId, label: t.retryHotspots },
          { id: 'blockades' as ViewId, label: t.eventBlockades }
        ]
      },
      {
        label: t.operations,
        children: [
          { id: 'search' as ViewId, label: t.inspectorTab }
        ]
      },
      {
        label: t.administration,
        children: [
          { id: 'admin-queues' as ViewId, label: t.adminQueues },
          { id: 'admin-actions' as ViewId, label: t.adminActions }
        ]
      }
    ];
  });

  readonly currentTitle = computed(() => {
    const t = this.service.t();

    switch (this.selectedView()) {
      case 'queue-states': return t.queueStateDist;
      case 'workers': return t.activeWorkers;
      case 'backlog': return t.backlogDepth;
      case 'wait-times': return t.waitTimes;
      case 'throughput': return t.throughput;
      case 'latency': return t.latencyPercentiles;
      case 'failures': return t.failureHotspots;
      case 'failed-tasks': return t.failedTaskList;
      case 'slow-tasks': return t.slowTasks;
      case 'retries': return t.retryHotspots;
      case 'blockades': return t.eventBlockades;
      case 'search': return t.inspectorTab;
      case 'admin-queues': return t.adminQueues;
      case 'admin-actions': return t.adminActions;
    }
  });

  readonly currentColumns = computed<EnterpriseColumn[]>(() => {
    const t = this.service.t();

    switch (this.selectedView()) {
      case 'queue-states':
        return [
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'state', label: t.state, kind: 'badge', value: x => x.state },
          { key: 'count', label: t.count, kind: 'number', align: 'right', value: x => x.count }
        ];

      case 'workers':
        return [
          { key: 'workerId', label: t.workerId, value: x => x.workerId },
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'activeRuns', label: t.runs, kind: 'number', align: 'right', value: x => x.activeRuns },
          { key: 'action', label: t.releaseWorkerClaims, kind: 'action', align: 'center', value: () => t.execute }
        ];

      case 'backlog':
        return [
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'oldestPendingAt', label: t.oldest, kind: 'date', value: x => this.formatDateTime(x.oldestPendingAt) },
          { key: 'pendingCount', label: t.pending, kind: 'number', align: 'right', value: x => x.pendingCount }
        ];

      case 'wait-times':
        return [
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'avgWaitTimeMs', label: t.avgWait, kind: 'number', align: 'right', value: x => Math.round(x.avgWaitTimeMs) },
          { key: 'maxWaitTimeMs', label: t.maxWait, kind: 'number', align: 'right', value: x => Math.round(x.maxWaitTimeMs) }
        ];

      case 'throughput':
        return [
          { key: 'timeBucket', label: t.timeBucket, kind: 'date', value: x => this.formatDateTime(x.timeBucket) },
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'completedCount', label: t.completedCount, kind: 'number', align: 'right', value: x => x.completedCount },
          { key: 'failedCount', label: t.failedCount, kind: 'number', align: 'right', value: x => x.failedCount },
          { key: 'avgDurationMs', label: t.avgDuration, kind: 'number', align: 'right', value: x => Math.round(x.avgDurationMs) }
        ];

      case 'latency':
        return [
          { key: 'taskName', label: t.taskName, value: x => x.taskName },
          { key: 'p50Ms', label: 'P50', kind: 'number', align: 'right', value: x => Math.round(x.p50Ms) },
          { key: 'p95Ms', label: 'P95', kind: 'number', align: 'right', value: x => Math.round(x.p95Ms) },
          { key: 'p99Ms', label: 'P99', kind: 'number', align: 'right', value: x => Math.round(x.p99Ms) }
        ];

      case 'failures':
        return [
          { key: 'taskName', label: t.taskName, value: x => x.taskName },
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'failureCount', label: t.fails, kind: 'number', align: 'right', value: x => x.failureCount },
          { key: 'lastFailedAt', label: t.lastFailed, kind: 'date', value: x => this.formatDateTime(x.lastFailedAt) }
        ];

      case 'failed-tasks':
        return [
          { key: 'taskId', label: t.taskId, value: x => this.shortId(x.taskId) },
          { key: 'taskName', label: t.taskName, value: x => x.taskName },
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'attempts', label: t.attempts, kind: 'number', align: 'right', value: x => x.attempts },
          { key: 'runId', label: t.runId, value: x => this.shortId(x.runId) },
          { key: 'failedAt', label: t.failedAt, kind: 'date', value: x => this.formatDateTime(x.failedAt) },
          { key: 'failureReason', label: t.failureReason, value: x => x.failureReason },
          { key: 'action', label: t.inspect, kind: 'action', align: 'center', value: () => t.viewJson }
        ];

      case 'slow-tasks':
        return [
          { key: 'taskId', label: t.taskId, value: x => this.shortId(x.taskId) },
          { key: 'taskName', label: t.taskName, value: x => x.taskName },
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'durationMs', label: t.duration, kind: 'number', align: 'right', value: x => Math.round(x.durationMs) },
          { key: 'completedAt', label: t.completedAt, kind: 'date', value: x => this.formatDateTime(x.completedAt) },
          { key: 'action', label: t.inspect, kind: 'action', align: 'center', value: () => t.viewJson }
        ];

      case 'retries':
        return [
          { key: 'taskName', label: t.taskName, value: x => x.taskName },
          { key: 'taskId', label: t.taskId, value: x => this.shortId(x.taskId) },
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'attempts', label: t.retries, kind: 'number', align: 'right', value: x => x.attempts },
          { key: 'state', label: t.state, kind: 'badge', value: x => x.state },
          { key: 'action', label: t.inspect, kind: 'action', align: 'center', value: () => t.viewJson }
        ];

      case 'blockades':
        return [
          { key: 'eventName', label: t.eventName, value: x => x.eventName },
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'waitingCount', label: t.blocked, kind: 'number', align: 'right', value: x => x.waitingCount },
          { key: 'oldestWaitAt', label: t.oldest, kind: 'date', value: x => this.formatDateTime(x.oldestWaitAt) }
        ];

      case 'search':
        return [
          { key: 'taskId', label: t.taskId, value: x => this.shortId(x.taskId) },
          { key: 'taskName', label: t.taskName, value: x => x.taskName },
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'state', label: t.state, kind: 'badge', value: x => x.state },
          { key: 'attempts', label: t.attempts, kind: 'number', align: 'right', value: x => x.attempts },
          { key: 'enqueuedAt', label: t.enqueuedAt, kind: 'date', value: x => this.formatDateTime(x.enqueuedAt) },
          { key: 'failureReason', label: t.failureReasonParams, value: x => x.failureReason || x.paramsJson },
          { key: 'action', label: t.inspect, kind: 'action', align: 'center', value: () => t.viewJson }
        ];

      case 'admin-queues':
        return [
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'action', label: t.dropQueue, kind: 'action', align: 'center', value: () => t.dropQueue }
        ];

      case 'admin-actions':
        return [];
    }
  });

  readonly currentRows = computed<any[]>(() => {
    const filter = this.clientFilter().trim().toLowerCase();

    const contains = (...values: Array<string | number | null | undefined>) => {
      if (!filter || this.selectedView() === 'search') return true;

      return values
        .filter(x => x !== null && x !== undefined)
        .some(x => String(x).toLowerCase().includes(filter));
    };

    switch (this.selectedView()) {
      case 'queue-states':
        return this.service.stats().filter(x => contains(x.queueName, x.state, x.count));

      case 'workers':
        return this.service.workers().filter(x => contains(x.workerId, x.queueName, x.activeRuns));

      case 'backlog':
        return this.service.backlog().filter(x => contains(x.queueName, x.pendingCount, x.oldestPendingAt));

      case 'wait-times':
        return this.service.waitTimes().filter(x => contains(x.queueName, x.avgWaitTimeMs, x.maxWaitTimeMs));

      case 'throughput':
        return this.service.throughput().filter(x => contains(x.timeBucket, x.queueName, x.completedCount, x.failedCount, x.avgDurationMs));

      case 'latency':
        return this.service.percentiles().filter(x => contains(x.taskName, x.p50Ms, x.p95Ms, x.p99Ms));

      case 'failures':
        return this.service.hotspots().filter(x => contains(x.taskName, x.queueName, x.failureCount, x.lastFailedAt));

      case 'failed-tasks':
        return this.service.failedTasks().filter(x => contains(x.taskId, x.taskName, x.queueName, x.attempts, x.runId, x.failedAt, x.failureReason));

      case 'slow-tasks':
        return this.service.slowTasks().filter(x => contains(x.taskId, x.taskName, x.queueName, x.durationMs, x.completedAt));

      case 'retries':
        return this.service.retryHotspots().filter(x => contains(x.taskName, x.taskId, x.queueName, x.state, x.attempts));

      case 'blockades':
        return this.service.activeWaits().filter(x => contains(x.eventName, x.queueName, x.waitingCount, x.oldestWaitAt));

      case 'search':
        return this.service.searchResults();

      case 'admin-queues':
        return this.service.adminQueues()
          .map(queueName => ({ queueName }))
          .filter(x => contains(x.queueName));

      case 'admin-actions':
        return [];
    }
  });

  readonly totalRows = computed(() => this.currentRows().length);

  ngOnInit(): void {
    this.refreshCurrentView();
    this.service.loadAdminQueues();

    interval(10000)
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => {
        if (this.service.autoRefreshEnabled()) {
          this.refreshCurrentView();
        }
      });
  }

  selectView(view: ViewId): void {
    this.selectedView.set(view);
    this.clientFilter.set('');
    this.refreshCurrentView();
  }

  refreshCurrentView(): void {
    switch (this.selectedView()) {
      case 'queue-states':
      case 'workers':
      case 'backlog':
        this.service.loadOverviewMetrics();
        break;

      case 'wait-times':
        this.service.loadOverviewMetrics();
        this.service.loadWaitTimes();
        break;

      case 'throughput':
        this.service.loadOverviewMetrics();
        this.service.loadThroughput();
        break;

      case 'latency':
      case 'failures':
      case 'retries':
      case 'blockades':
        this.service.loadOverviewMetrics();
        this.service.loadAnalytics();
        break;

      case 'failed-tasks':
        this.service.loadOverviewMetrics();
        this.service.loadFailedTasks();
        break;

      case 'slow-tasks':
        this.service.loadOverviewMetrics();
        this.service.loadSlowTasks();
        break;

      case 'search':
        this.service.loadOverviewMetrics();
        this.service.searchTasks();
        break;

      case 'admin-queues':
        this.service.loadAdminQueues();
        break;

      case 'admin-actions':
        this.service.loadAdminQueues();
        this.service.loadOverviewMetrics();
        break;
    }
  }

  onFilterChange(value: string): void {
    this.clientFilter.set(value);

    if (this.selectedView() === 'search') {
      this.service.searchTerm.set(value);
      this.service.page.set(1);
      this.service.searchTasks();
    }
  }

  onFetchLimitChange(limit: number): void {
    this.service.fetchLimit.set(limit);
    this.refreshCurrentView();
  }

  onThroughputWindowChange(seconds: number): void {
    this.service.throughputWindowSeconds.set(seconds);
    this.service.loadThroughput();
  }

  onSearchPageSizeChange(size: number): void {
    this.service.pageSize.set(size);
    this.service.page.set(1);
    this.service.searchTasks();
  }

  onSearchQueueChange(queueName: string): void {
    this.service.selectedQueue.set(queueName);
    this.service.page.set(1);
    this.service.loadDatabaseHealthForSelectedQueue();
    this.service.searchTasks();
  }

  onSearchStateChange(state: string): void {
    this.service.selectedState.set(state);
    this.service.page.set(1);
    this.service.searchTasks();
  }

  previousPage(): void {
    this.service.previousSearchPage();
  }

  nextPage(): void {
    this.service.nextSearchPage();
  }

  createQueueFromToolbar(): void {
    const queueName = this.newQueueName().trim();

    if (!queueName) return;

    this.service.createQueue({
      queueName,
      storageMode: this.newQueueStorageMode()
    });

    this.newQueueName.set('');
    this.newQueueStorageMode.set('unpartitioned');
  }

  onRowAction(row: any): void {
    if (this.selectedView() === 'admin-queues') {
      if (!confirm(this.service.t().deleteQueueConfirm)) return;
      this.service.dropQueue(row.queueName);
      return;
    }

    if (this.selectedView() === 'workers') {
      if (!confirm(this.service.t().releaseClaimsConfirm)) return;

      this.service.releaseWorkerClaims({
        queueName: row.queueName,
        workerId: row.workerId
      });

      return;
    }

    if (!row?.queueName || !row?.taskId) return;

    this.service.fetchTaskDetails(row.queueName, row.taskId);
  }

  cancelSelectedTask(): void {
    const task = this.service.selectedTaskDetail();

    if (!task) return;
    if (!confirm(this.service.t().cancelTaskConfirm)) return;

    this.service.cancelTask({
      queueName: task.queueName,
      taskId: task.taskId
    });

    this.service.selectedTaskDetail.set(null);
  }

  executeAdminOperation(): void {
    const queueName = this.adminQueueName();

    switch (this.adminOperation()) {
      case 'cancel-task':
        this.service.cancelTask({ queueName, taskId: this.adminTaskId() });
        break;

      case 'bulk-cancel':
        this.service.bulkCancelTasks({ queueName, taskIds: this.parseTaskIds(this.adminTaskIdsText()) });
        break;

      case 'release-worker-claims':
        this.service.releaseWorkerClaims({ queueName, workerId: this.adminWorkerId() });
        break;

      case 'extend-claim':
        this.service.extendClaim({
          queueName,
          runId: this.adminRunId(),
          extendBySeconds: Number(this.adminExtendBySeconds() || 30)
        });
        break;

      case 'wake-run':
        this.service.wakeRun({
          queueName,
          runId: this.adminRunId(),
          wakeAt: new Date(this.adminWakeAt()).toISOString()
        });
        break;

      case 'complete-run':
        this.service.completeRun({
          queueName,
          runId: this.adminRunId(),
          stateJson: this.adminStateJson() || null
        });
        break;

      case 'fail-run':
        this.service.failRun({
          queueName,
          runId: this.adminRunId(),
          reasonJson: this.adminReasonJson(),
          retryAt: this.adminRetryAt() ? new Date(this.adminRetryAt()).toISOString() : null
        });
        break;

      case 'emit-event':
        this.service.emitEvent({
          queueName,
          eventName: this.adminEventName(),
          payloadJson: this.adminPayloadJson() || null
        });
        break;

      case 'set-checkpoint':
        this.service.setCheckpoint({
          queueName,
          taskId: this.adminTaskId(),
          ownerRunId: this.adminOwnerRunId(),
          stepName: this.adminStepName(),
          stateJson: this.adminCheckpointStateJson(),
          extendClaimBySeconds: this.adminExtendCheckpointBySeconds()
        });
        break;

      case 'cleanup-tasks':
        this.service.cleanupTasks({
          queueName,
          ttlSeconds: Number(this.adminTtlSeconds()),
          limit: Number(this.adminCleanupLimit() || 1000)
        });
        break;

      case 'cleanup-events':
        this.service.cleanupEvents({
          queueName,
          ttlSeconds: Number(this.adminTtlSeconds()),
          limit: Number(this.adminCleanupLimit() || 1000)
        });
        break;
    }
  }

  canExecuteAdminOperation(): boolean {
    if (!this.adminQueueName()) return false;

    switch (this.adminOperation()) {
      case 'cancel-task':
        return !!this.adminTaskId();

      case 'bulk-cancel':
        return this.parseTaskIds(this.adminTaskIdsText()).length > 0;

      case 'release-worker-claims':
        return !!this.adminWorkerId();

      case 'extend-claim':
      case 'wake-run':
      case 'complete-run':
        return !!this.adminRunId();

      case 'fail-run':
        return !!this.adminRunId() && !!this.adminReasonJson();

      case 'emit-event':
        return !!this.adminEventName();

      case 'set-checkpoint':
        return !!this.adminTaskId()
          && !!this.adminOwnerRunId()
          && !!this.adminStepName()
          && !!this.adminCheckpointStateJson();

      case 'cleanup-tasks':
      case 'cleanup-events':
        return Number(this.adminTtlSeconds()) > 0;
    }
  }

  requiresTaskId(): boolean {
    return this.adminOperation() === 'cancel-task'
      || this.adminOperation() === 'set-checkpoint';
  }

  requiresRunId(): boolean {
    return this.adminOperation() === 'extend-claim'
      || this.adminOperation() === 'wake-run'
      || this.adminOperation() === 'complete-run'
      || this.adminOperation() === 'fail-run';
  }

  private parseTaskIds(value: string): string[] {
    return value
      .split(/[\n,;]/)
      .map(x => x.trim())
      .filter(Boolean);
  }

  cell(row: any, col: EnterpriseColumn): any {
    return col.value(row);
  }

  emptyMessage(): string {
    const t = this.service.t();

    switch (this.selectedView()) {
      case 'queue-states':
      case 'backlog':
        return t.noQueueData;
      case 'workers':
        return t.noActiveWorkers;
      case 'wait-times':
        return t.noWaitTimes;
      case 'throughput':
        return t.noThroughput;
      case 'latency':
        return t.noLatencyData;
      case 'failures':
        return t.noHotspots;
      case 'failed-tasks':
        return t.noFailedTaskList;
      case 'slow-tasks':
        return t.noSlowTasks;
      case 'retries':
        return t.noRetryHotspots;
      case 'blockades':
        return t.noBlockades;
      case 'search':
        return t.noMatchingTasks;
      case 'admin-queues':
        return t.noAdminQueues;
      case 'admin-actions':
        return '';
    }
  }

  badgeClass(value: any): string {
    switch (String(value).toLowerCase()) {
      case 'pending':
        return 'bg-amber-50 text-amber-700 border-amber-200';
      case 'running':
        return 'bg-sky-50 text-sky-700 border-sky-200';
      case 'completed':
        return 'bg-emerald-50 text-emerald-700 border-emerald-200';
      case 'failed':
        return 'bg-rose-50 text-rose-700 border-rose-200';
      case 'sleeping':
        return 'bg-indigo-50 text-indigo-700 border-indigo-200';
      case 'cancelled':
      case 'canceled':
        return 'bg-slate-100 text-slate-700 border-slate-300';
      default:
        return 'bg-slate-50 text-slate-700 border-slate-200';
    }
  }

  shortId(value: string | null | undefined): string {
    if (!value) return '-';
    return value.length > 12 ? value.slice(0, 12) : value;
  }

  formatDateTime(value: string | null | undefined): string {
    if (!value) return '-';

    const date = new Date(value);

    if (Number.isNaN(date.getTime())) {
      return value;
    }

    return date.toLocaleString();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
