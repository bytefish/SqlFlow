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
import { Subject, EMPTY, catchError, forkJoin, interval, of, takeUntil } from 'rxjs';

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

export interface UpcomingWakeupBucketItem {
  timeBucket: string;
  queueName: string;
  sleepingCount: number;
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

export interface ControlPanelTranslations {
  title: string;
  subtitle: string;
  autoRefresh: string;
  refresh: string;
  language: string;
  quickFilter: string;

  // Navigation groups
  monitoring: string;
  analytics: string;
  operations: string;

  // KPI strip
  backlog: string;
  activeExecutions: string;
  completed: string;
  failedTasks: string;
  dbStorage: string;

  // Queue states
  queueStateDist: string;
  queue: string;
  state: string;
  count: string;
  noQueueData: string;

  // Workers
  activeWorkers: string;
  workerId: string;
  runs: string;
  noActiveWorkers: string;

  // Backlog
  backlogDepth: string;
  oldest: string;
  pending: string;

  // Wait times
  waitTimes: string;
  avgWait: string;
  maxWait: string;
  noWaitTimes: string;

  // Throughput
  throughput: string;
  timeBucket: string;
  completedCount: string;
  failedCount: string;
  avgDuration: string;
  noThroughput: string;

  // Latency
  latencyPercentiles: string;
  noLatencyData: string;

  // Failure hotspots
  failureHotspots: string;
  fails: string;
  lastFailed: string;
  noHotspots: string;

  // Failed task listing
  failedTaskList: string;
  failedAt: string;
  failureReason: string;
  failureReasonParams: string;
  noFailedTaskList: string;

  // Slow tasks
  slowTasks: string;
  duration: string;
  completedAt: string;
  noSlowTasks: string;

  // Retry hotspots
  retryHotspots: string;
  retries: string;
  noRetryHotspots: string;

  // Event blockades
  eventBlockades: string;
  eventName: string;
  blocked: string;
  noBlockades: string;

  // Inspector / search
  inspectorTab: string;
  queueLabel: string;
  stateLabel: string;
  allStates: string;
  searchPlaceholder: string;
  executeQuery: string;
  noMatchingTasks: string;

  // Generic task columns
  taskId: string;
  taskName: string;
  attempts: string;
  enqueuedAt: string;
  inspect: string;
  viewJson: string;

  // Pagination
  prev: string;
  next: string;
  page: string;

  // Modal
  parametersJson: string;
  completedPayloadResult: string;
  close: string;

  // Toolbar / footer
  pageSize: string;
  serverLimit: string;
  window: string;
  rows: string;
  loaded: string;
  visible: string;
}

export const TRANSLATIONS_EN: ControlPanelTranslations = {
  title: 'SqlFlow Control Panel',
  subtitle: 'Workflow Admin Console',
  autoRefresh: 'Auto-refresh (10s)',
  refresh: 'Refresh',
  language: 'Language',
  quickFilter: 'Quick filter...',

  monitoring: 'Monitoring',
  analytics: 'Analytics',
  operations: 'Operations',

  backlog: 'Backlog',
  activeExecutions: 'Running',
  completed: 'Completed',
  failedTasks: 'Failed',
  dbStorage: 'DB Storage',

  queueStateDist: 'Queue States',
  queue: 'Queue',
  state: 'State',
  count: 'Count',
  noQueueData: 'No data.',

  activeWorkers: 'Active Workers',
  workerId: 'Worker ID',
  runs: 'Runs',
  noActiveWorkers: 'No active workers.',

  backlogDepth: 'Backlog Depth',
  oldest: 'Oldest',
  pending: 'Pending',

  waitTimes: 'Wait Times',
  avgWait: 'Avg Wait',
  maxWait: 'Max Wait',
  noWaitTimes: 'No wait time data available.',

  throughput: 'Throughput',
  timeBucket: 'Time Bucket',
  completedCount: 'Completed',
  failedCount: 'Failed',
  avgDuration: 'Avg Duration',
  noThroughput: 'No throughput data available.',

  latencyPercentiles: 'Latency Percentiles',
  noLatencyData: 'No latency data available.',

  failureHotspots: 'Failure Hotspots',
  fails: 'Fails',
  lastFailed: 'Last Failed',
  noHotspots: 'No failure hotspots.',

  failedTaskList: 'Failed Tasks',
  failedAt: 'Failed At',
  failureReason: 'Failure Reason',
  failureReasonParams: 'Params / Error',
  noFailedTaskList: 'No failed tasks available.',

  slowTasks: 'Slow Tasks',
  duration: 'Duration',
  completedAt: 'Completed At',
  noSlowTasks: 'No slow tasks available.',

  retryHotspots: 'Retry Hotspots',
  retries: 'Retries',
  noRetryHotspots: 'No retry hotspots.',

  eventBlockades: 'Event Blockades',
  eventName: 'Event Name',
  blocked: 'Blocked',
  noBlockades: 'No event blockades.',

  inspectorTab: 'Advanced Task Search',
  queueLabel: 'Queue',
  stateLabel: 'State',
  allStates: 'All States',
  searchPlaceholder: 'Task name, error or JSON...',
  executeQuery: 'Run Query',
  noMatchingTasks: 'No matching tasks found.',

  taskId: 'Task ID',
  taskName: 'Task Name',
  attempts: 'Attempts',
  enqueuedAt: 'Enqueued',
  inspect: 'Action',
  viewJson: 'JSON',

  prev: 'Previous',
  next: 'Next',
  page: 'Page',

  parametersJson: 'Parameters JSON',
  completedPayloadResult: 'Result Payload JSON',
  close: 'Close',

  pageSize: 'Page Size',
  serverLimit: 'Limit',
  window: 'Window',
  rows: 'Rows',
  loaded: 'loaded',
  visible: 'visible'
};

export const TRANSLATIONS_DE: ControlPanelTranslations = {
  title: 'SqlFlow Control Panel',
  subtitle: 'Workflow Admin Console',
  autoRefresh: 'Auto-Refresh (10s)',
  refresh: 'Aktualisieren',
  language: 'Sprache',
  quickFilter: 'Schnellfilter...',

  monitoring: 'Monitoring',
  analytics: 'Analyse',
  operations: 'Operations',

  backlog: 'Backlog',
  activeExecutions: 'Laufend',
  completed: 'Erledigt',
  failedTasks: 'Fehler',
  dbStorage: 'DB-Größe',

  queueStateDist: 'Queue-Zustände',
  queue: 'Queue',
  state: 'Zustand',
  count: 'Anzahl',
  noQueueData: 'Keine Daten vorhanden.',

  activeWorkers: 'Aktive Worker',
  workerId: 'Worker ID',
  runs: 'Runs',
  noActiveWorkers: 'Keine aktiven Worker vorhanden.',

  backlogDepth: 'Backlog-Tiefe',
  oldest: 'Ältester',
  pending: 'Ausstehend',

  waitTimes: 'Wartezeiten',
  avgWait: 'Ø Wartezeit',
  maxWait: 'Max. Wartezeit',
  noWaitTimes: 'Keine Wartezeiten vorhanden.',

  throughput: 'Durchsatz',
  timeBucket: 'Zeitfenster',
  completedCount: 'Erledigt',
  failedCount: 'Fehler',
  avgDuration: 'Ø Dauer',
  noThroughput: 'Keine Durchsatzdaten vorhanden.',

  latencyPercentiles: 'Latenz-Perzentile',
  noLatencyData: 'Keine Latenzdaten vorhanden.',

  failureHotspots: 'Fehler-Hotspots',
  fails: 'Fehler',
  lastFailed: 'Zuletzt fehlgeschlagen',
  noHotspots: 'Keine Fehler-Hotspots vorhanden.',

  failedTaskList: 'Fehlgeschlagene Tasks',
  failedAt: 'Fehlgeschlagen am',
  failureReason: 'Fehlergrund',
  failureReasonParams: 'Parameter / Fehler',
  noFailedTaskList: 'Keine fehlgeschlagenen Tasks vorhanden.',

  slowTasks: 'Langsame Tasks',
  duration: 'Dauer',
  completedAt: 'Abgeschlossen am',
  noSlowTasks: 'Keine langsamen Tasks vorhanden.',

  retryHotspots: 'Retry-Hotspots',
  retries: 'Retries',
  noRetryHotspots: 'Keine Retry-Hotspots vorhanden.',

  eventBlockades: 'Event-Blockaden',
  eventName: 'Event Name',
  blocked: 'Blockiert',
  noBlockades: 'Keine Event-Blockaden vorhanden.',

  inspectorTab: 'Erweiterte Task-Suche',
  queueLabel: 'Queue',
  stateLabel: 'Zustand',
  allStates: 'Alle Zustände',
  searchPlaceholder: 'Task-Name, Fehler oder JSON...',
  executeQuery: 'Abfrage ausführen',
  noMatchingTasks: 'Keine passenden Tasks gefunden.',

  taskId: 'Task ID',
  taskName: 'Task Name',
  attempts: 'Versuche',
  enqueuedAt: 'Eingereiht',
  inspect: 'Aktion',
  viewJson: 'JSON',

  prev: 'Zurück',
  next: 'Weiter',
  page: 'Seite',

  parametersJson: 'Parameter JSON',
  completedPayloadResult: 'Ergebnis-Payload JSON',
  close: 'Schließen',

  pageSize: 'Seitengröße',
  serverLimit: 'Limit',
  window: 'Zeitraum',
  rows: 'Zeilen',
  loaded: 'geladen',
  visible: 'sichtbar'
};

export const TRANSLATIONS_ZH: ControlPanelTranslations = {
  title: 'SqlFlow 系统浏览器',
  subtitle: '企业级工作流遥测控制台',
  autoRefresh: '自动刷新 (10s)',
  refresh: '刷新',
  language: '语言',
  quickFilter: '快速筛选...',

  monitoring: '监控',
  analytics: '分析',
  operations: '操作',

  backlog: '积压',
  activeExecutions: '运行中',
  completed: '已完成',
  failedTasks: '失败',
  dbStorage: '数据库大小',

  queueStateDist: '队列状态',
  queue: '队列',
  state: '状态',
  count: '数量',
  noQueueData: '无数据。',

  activeWorkers: '活跃工作节点',
  workerId: '工作节点 ID',
  runs: '运行',
  noActiveWorkers: '无活跃工作节点。',

  backlogDepth: '积压深度',
  oldest: '最早',
  pending: '等待中',

  waitTimes: '等待时间',
  avgWait: '平均等待',
  maxWait: '最大等待',
  noWaitTimes: '无等待时间数据。',

  throughput: '吞吐量',
  timeBucket: '时间窗口',
  completedCount: '已完成',
  failedCount: '失败',
  avgDuration: '平均耗时',
  noThroughput: '无吞吐量数据。',

  latencyPercentiles: '延迟百分位数',
  noLatencyData: '无延迟数据。',

  failureHotspots: '失败热点',
  fails: '失败',
  lastFailed: '最后失败',
  noHotspots: '无失败热点。',

  failedTaskList: '失败任务',
  failedAt: '失败时间',
  failureReason: '失败原因',
  failureReasonParams: '参数 / 错误',
  noFailedTaskList: '无失败任务。',

  slowTasks: '慢任务',
  duration: '耗时',
  completedAt: '完成时间',
  noSlowTasks: '无慢任务。',

  retryHotspots: '重试热点',
  retries: '重试',
  noRetryHotspots: '无重试热点。',

  eventBlockades: '事件阻塞',
  eventName: '事件名称',
  blocked: '已阻塞',
  noBlockades: '无事件阻塞。',

  inspectorTab: '高级任务搜索',
  queueLabel: '队列',
  stateLabel: '状态',
  allStates: '所有状态',
  searchPlaceholder: '任务名称、错误或 JSON...',
  executeQuery: '执行查询',
  noMatchingTasks: '未找到匹配任务。',

  taskId: '任务 ID',
  taskName: '任务名称',
  attempts: '尝试次数',
  enqueuedAt: '入队时间',
  inspect: '操作',
  viewJson: 'JSON',

  prev: '上一页',
  next: '下一页',
  page: '页',

  parametersJson: '参数 JSON',
  completedPayloadResult: '结果载荷 JSON',
  close: '关闭',

  pageSize: '每页条数',
  serverLimit: '限制',
  window: '时间范围',
  rows: '行',
  loaded: '已加载',
  visible: '可见'
};

/* ============================================================
   Services
   ============================================================ */
export interface AppSettings {
  sqlFlowManagementApiUrl: string;
}

@Injectable({
  providedIn: 'root'
})
export class AppSettingsService {
  private readonly settingsSignal = signal<AppSettings | null>(null);

  readonly settings = this.settingsSignal.asReadonly();

  async load(): Promise<void> {
    const response = await fetch('/appsettings.json', {
      cache: 'no-store'
    });

    if (!response.ok) {
      throw new Error(`Could not load appsettings.json. Status: ${response.status}`);
    }

    const settings = await response.json() as AppSettings;

    if (!settings.sqlFlowManagementApiUrl) {
      throw new Error('Missing setting: sqlFlowManagementApiUrl');
    }

    this.settingsSignal.set(settings);
  }

  get sqlFlowManagementApiUrl(): string {
    const settings = this.settingsSignal();

    if (!settings) {
      throw new Error('App settings have not been loaded yet.');
    }

    return settings.sqlFlowManagementApiUrl;
  }
}

@Injectable({ providedIn: 'root' })
export class SqlFlowManagementService {
  private readonly http = inject(HttpClient);
  private readonly appSettings = inject(AppSettingsService);

  readonly currentLanguage = signal<'en' | 'de' | 'zh'>('en');

  readonly t = computed(() => {
    switch (this.currentLanguage()) {
      case 'de':
        return TRANSLATIONS_DE;
      case 'zh':
        return TRANSLATIONS_ZH;
      default:
        return TRANSLATIONS_EN;
    }
  });

  private get baseUrl(): string {
    return this.appSettings.sqlFlowManagementApiUrl;
  }

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

  readonly isLoading = signal(false);
  readonly isDetailLoading = signal(false);
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
    this.stats()
      .filter(x => x.state === 'pending')
      .reduce((sum, x) => sum + x.count, 0)
  );

  readonly totalRunning = computed(() =>
    this.stats()
      .filter(x => x.state === 'running')
      .reduce((sum, x) => sum + x.count, 0)
  );

  readonly totalFailed = computed(() =>
    this.stats()
      .filter(x => x.state === 'failed')
      .reduce((sum, x) => sum + x.count, 0)
  );

  readonly totalCompleted = computed(() =>
    this.stats()
      .filter(x => x.state === 'completed')
      .reduce((sum, x) => sum + x.count, 0)
  );

  readonly availableQueues = computed(() => {
    const queues = this.stats().map(x => x.queueName);
    return Array.from(new Set(queues)).sort();
  });

  private handleError(error: any) {
    this.isLoading.set(false);
    this.isDetailLoading.set(false);

    console.log(error)

    const msg = error?.error?.message || error?.message || 'API connection failed.';
    this.errorMessage.set(msg);

    return of([]);
  }

  loadOverviewMetrics(): void {
    this.isLoading.set(true);
    this.errorMessage.set(null);

    forkJoin({
      stats: this.http
        .get<QueueStatItem[]>(`${this.baseUrl}/stats`)
        .pipe(catchError(err => this.handleError(err))),

      workers: this.http
        .get<ActiveWorkerItem[]>(`${this.baseUrl}/workers`)
        .pipe(catchError(err => this.handleError(err))),

      backlog: this.http
        .get<QueueBacklogItem[]>(`${this.baseUrl}/backlog?limit=${this.fetchLimit()}`)
        .pipe(catchError(err => this.handleError(err)))
    }).subscribe(result => {
      this.stats.set(result.stats as QueueStatItem[]);
      this.workers.set(result.workers as ActiveWorkerItem[]);
      this.backlog.set(result.backlog as QueueBacklogItem[]);

      if (!this.selectedQueue() && this.stats().length > 0) {
        this.selectedQueue.set(this.stats()[0].queueName);
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
      ? `${this.baseUrl}/latency-percentiles?queueName=${encodeURIComponent(queueName)}`
      : `${this.baseUrl}/latency-percentiles`;

    forkJoin({
      percentiles: this.http
        .get<TaskPercentileItem[]>(latencyUrl)
        .pipe(catchError(err => this.handleError(err))),

      hotspots: this.http
        .get<TaskFailureHotspotItem[]>(`${this.baseUrl}/hotspots/failures?lookbackSeconds=86400&limit=${limit}`)
        .pipe(catchError(err => this.handleError(err))),

      retries: this.http
        .get<RetryHotspotItem[]>(`${this.baseUrl}/hotspots/retries?limit=${limit}`)
        .pipe(catchError(err => this.handleError(err))),

      activeWaits: this.http
        .get<ActiveWaitItem[]>(`${this.baseUrl}/active-waits?limit=${limit}`)
        .pipe(catchError(err => this.handleError(err)))
    }).subscribe(result => {
      this.percentiles.set(result.percentiles as TaskPercentileItem[]);
      this.hotspots.set(result.hotspots as TaskFailureHotspotItem[]);
      this.retryHotspots.set(result.retries as RetryHotspotItem[]);
      this.activeWaits.set(result.activeWaits as ActiveWaitItem[]);

      this.isLoading.set(false);
    });
  }

  loadWaitTimes(): void {
    this.isLoading.set(true);
    this.errorMessage.set(null);

    this.http
      .get<QueueWaitTimeItem[]>(`${this.baseUrl}/wait-times?limit=${this.fetchLimit()}`)
      .pipe(catchError(err => this.handleError(err)))
      .subscribe(result => {
        this.waitTimes.set(result as QueueWaitTimeItem[]);
        this.isLoading.set(false);
      });
  }

  loadThroughput(): void {
    this.isLoading.set(true);
    this.errorMessage.set(null);

    this.http
      .get<ThroughputBucketItem[]>(
        `${this.baseUrl}/throughput?windowSeconds=${this.throughputWindowSeconds()}`
      )
      .pipe(catchError(err => this.handleError(err)))
      .subscribe(result => {
        this.throughput.set(result as ThroughputBucketItem[]);
        this.isLoading.set(false);
      });
  }

  loadSlowTasks(): void {
    this.isLoading.set(true);
    this.errorMessage.set(null);

    this.http
      .get<SlowTaskItem[]>(`${this.baseUrl}/slow-tasks?limit=${this.fetchLimit()}`)
      .pipe(catchError(err => this.handleError(err)))
      .subscribe(result => {
        this.slowTasks.set(result as SlowTaskItem[]);
        this.isLoading.set(false);
      });
  }

  loadFailedTasks(): void {
    this.isLoading.set(true);
    this.errorMessage.set(null);

    this.http
      .get<FailedTaskItem[]>(`${this.baseUrl}/failed-tasks?limit=${this.fetchLimit()}`)
      .pipe(catchError(err => this.handleError(err)))
      .subscribe(result => {
        this.failedTasks.set(result as FailedTaskItem[]);
        this.isLoading.set(false);
      });
  }

  loadDatabaseHealthForSelectedQueue(): void {
    const queueName = this.selectedQueue();

    if (!queueName) {
      this.dbHealth.set(null);
      return;
    }

    this.http
      .get<DatabaseHealthItem>(
        `${this.baseUrl}/health?queueName=${encodeURIComponent(queueName)}`
      )
      .pipe(
        catchError(() => {
          this.dbHealth.set(null);
          return EMPTY;
        })
      )
      .subscribe(result => this.dbHealth.set(result));
  }

  searchTasks(): void {
    if (!this.selectedQueue()) {
      this.searchResults.set([]);
      return;
    }

    this.isLoading.set(true);
    this.errorMessage.set(null);

    const filter: TaskSearchFilter = {
      queueName: this.selectedQueue(),
      states: this.selectedState() !== 'all'
        ? [this.selectedState()]
        : undefined,
      searchTerm: this.searchTerm() || undefined,
      minAttempts: this.minAttempts() ?? undefined,
      maxAttempts: this.maxAttempts() ?? undefined,
      claimedBy: this.claimedBy() || undefined,
      fromDate: this.fromDate()
        ? new Date(this.fromDate()).toISOString()
        : undefined,
      toDate: this.toDate()
        ? new Date(this.toDate()).toISOString()
        : undefined,
      sortBy: this.sortBy(),
      sortDescending: this.sortDescending(),
      offset: (this.page() - 1) * this.pageSize(),
      limit: this.pageSize()
    };

    this.http
      .post<TaskSearchResultItem[]>(`${this.baseUrl}/tasks/search`, filter)
      .pipe(catchError(err => this.handleError(err)))
      .subscribe(result => {
        this.searchResults.set(result as TaskSearchResultItem[]);
        this.isLoading.set(false);
      });
  }

  fetchTaskDetails(queueName: string, taskId: string): void {
    this.isDetailLoading.set(true);
    this.errorMessage.set(null);

    this.http
      .get<TaskDetailItem>(
        `${this.baseUrl}/tasks/${encodeURIComponent(queueName)}/${encodeURIComponent(taskId)}`
      )
      .pipe(
        catchError(() => {
          this.isDetailLoading.set(false);
          this.errorMessage.set(`Task details failed for ${taskId}`);
          return EMPTY;
        })
      )
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
  | 'search';

type ColumnKind =
  | 'text'
  | 'number'
  | 'badge'
  | 'date'
  | 'action';

interface EnterpriseColumn {
  key: string;
  label: string;
  kind?: ColumnKind;
  align?: 'left' | 'right' | 'center';
  value: (row: any) => string | number | null | undefined;
}

@Component({
  selector: 'app-root',
  imports: [CommonModule, FormsModule],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="h-screen w-full flex flex-col bg-slate-100 text-slate-900 font-sans overflow-hidden text-sm">

      <!-- Header -->
      <header class="flex-none flex items-center justify-between px-6 py-4 border-b border-slate-300 bg-white">
        <div class="flex items-center gap-4">
          <div class="h-9 w-9 rounded-lg bg-slate-900 flex items-center justify-center font-bold text-base text-white">
            ⚡
          </div>

          <div>
            <h1 class="text-xl font-bold text-slate-950">
              {{ service.t().title }}
            </h1>

            <p class="text-sm text-slate-500 hidden md:block">
              {{ service.t().subtitle }}
            </p>
          </div>
        </div>

        <div class="flex items-center gap-5">
          @if (service.errorMessage()) {
            <span class="text-rose-600 font-bold truncate max-w-sm" [title]="service.errorMessage()">
              ⚠️ {{ service.errorMessage() }}
            </span>
          }

          <div class="flex items-center gap-2">
            <span class="text-slate-600 font-semibold">
              {{ service.t().language }}:
            </span>

            <select
              [ngModel]="service.currentLanguage()"
              (ngModelChange)="service.currentLanguage.set($event)"
              class="bg-white border border-slate-300 rounded-md px-2 py-1 font-semibold">
              <option value="en">EN</option>
              <option value="de">DE</option>
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
            [disabled]="service.isLoading()"
            class="bg-slate-900 hover:bg-slate-800 disabled:opacity-50 text-white px-4 py-2 rounded-md font-bold transition-colors flex items-center gap-2">

            <svg
              class="w-4 h-4"
              [class.animate-spin]="service.isLoading()"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor">
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>

            {{ service.t().refresh }}
          </button>
        </div>
      </header>

      <!-- KPI Strip -->
      <div class="flex-none flex items-center gap-8 px-6 py-3 bg-white border-b border-slate-300 overflow-x-auto shadow-sm">

        <button
          (click)="selectView('backlog')"
          class="flex items-center gap-2 hover:bg-slate-100 px-2 py-1 rounded-md">
          <span class="font-bold text-slate-500 uppercase tracking-wider">
            {{ service.t().backlog }}:
          </span>
          <span class="font-mono font-bold text-amber-600 text-base">
            {{ service.totalPending() | number }}
          </span>
        </button>

        <div class="w-px h-5 bg-slate-300"></div>

        <button
          (click)="selectView('workers')"
          class="flex items-center gap-2 hover:bg-slate-100 px-2 py-1 rounded-md">
          <span class="font-bold text-slate-500 uppercase tracking-wider">
            {{ service.t().activeExecutions }}:
          </span>
          <span class="font-mono font-bold text-sky-600 text-base">
            {{ service.totalRunning() | number }}
          </span>
        </button>

        <div class="w-px h-5 bg-slate-300"></div>

        <button
          (click)="selectView('queue-states')"
          class="flex items-center gap-2 hover:bg-slate-100 px-2 py-1 rounded-md">
          <span class="font-bold text-slate-500 uppercase tracking-wider">
            {{ service.t().completed }}:
          </span>
          <span class="font-mono font-bold text-emerald-600 text-base">
            {{ service.totalCompleted() | number }}
          </span>
        </button>

        <div class="w-px h-5 bg-slate-300"></div>

        <button
          (click)="selectView('failed-tasks')"
          class="flex items-center gap-2 hover:bg-slate-100 px-2 py-1 rounded-md">
          <span class="font-bold text-slate-500 uppercase tracking-wider">
            {{ service.t().failedTasks }}:
          </span>
          <span class="font-mono font-bold text-rose-600 text-base">
            {{ service.totalFailed() | number }}
          </span>
        </button>

        <div class="w-px h-5 bg-slate-300"></div>

        <div class="flex items-center gap-2">
          <span class="font-bold text-slate-500 uppercase tracking-wider">
            {{ service.t().dbStorage }}:
          </span>

          <span class="font-mono font-bold text-slate-800 text-base">
            {{
              service.dbHealth()
                ? ((service.dbHealth()!.tasksTableBytes + service.dbHealth()!.runsTableBytes) / 1024 / 1024 | number:'1.1-1') + ' MB'
                : '-'
            }}
          </span>
        </div>
      </div>

      <!-- Main -->
      <main class="flex-1 flex overflow-hidden">

        <!-- Tree -->
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

        <!-- Content -->
        <section class="flex-1 overflow-hidden flex flex-col">

          <!-- Toolbar -->
          <div class="flex-none bg-white border-b border-slate-300 px-6 py-4">
            <div class="flex items-center justify-between gap-4">

              <div>
                <h2 class="text-lg font-bold text-slate-950">
                  {{ currentTitle() }}
                </h2>

                <p class="text-sm text-slate-500">
                  {{ totalRows() | number }} {{ service.t().rows }}
                </p>
              </div>

              <div class="flex items-center gap-3">

                @if (selectedView() === 'search') {
                  <select
                    [ngModel]="service.selectedQueue()"
                    (ngModelChange)="onSearchQueueChange($event)"
                    class="h-10 px-3 text-sm border border-slate-300 rounded-md bg-slate-50">
                    <option value="">
                      {{ service.t().queueLabel }}
                    </option>

                    @for (queue of service.availableQueues(); track queue) {
                      <option [value]="queue">
                        {{ queue }}
                      </option>
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

                <input
                  type="text"
                  [ngModel]="clientFilter()"
                  (ngModelChange)="onFilterChange($event)"
                  [placeholder]="selectedView() === 'search' ? service.t().searchPlaceholder : service.t().quickFilter"
                  class="h-10 px-3 text-sm border border-slate-300 rounded-md bg-slate-50 w-72 outline-none focus:ring-2 focus:ring-slate-400" />

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
                } @else {
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

                <button
                  (click)="refreshCurrentView()"
                  [disabled]="service.isLoading()"
                  class="h-10 px-4 bg-slate-900 hover:bg-slate-800 disabled:opacity-50 text-white rounded-md font-bold transition-colors">

                  {{ selectedView() === 'search' ? service.t().executeQuery : service.t().refresh }}
                </button>
              </div>
            </div>
          </div>

          <!-- Single Listing -->
          <div class="flex-1 overflow-auto p-6">
            <div class="bg-white border border-slate-300 shadow-sm rounded-lg overflow-hidden flex flex-col min-h-full">

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
                                <span
                                  class="block truncate max-w-[620px]"
                                  [title]="cell(row, col)">
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

              <!-- Footer -->
              <div class="flex-none bg-slate-50 border-t border-slate-200 px-4 py-3 flex justify-between items-center text-sm">

                <div class="text-slate-500 font-semibold">
                  @if (selectedView() === 'search') {
                    {{ service.t().page }} {{ service.page() }}
                    <span class="text-slate-400">
                      · {{ service.searchResults().length | number }} {{ service.t().loaded }}
                    </span>
                  } @else if (selectedView() === 'throughput') {
                    {{ service.t().window }} {{ service.throughputWindowSeconds() }}s
                    <span class="text-slate-400">
                      · {{ currentRows().length | number }} {{ service.t().visible }}
                    </span>
                  } @else {
                    {{ service.t().serverLimit }} {{ service.fetchLimit() }}
                    <span class="text-slate-400">
                      · {{ currentRows().length | number }} {{ service.t().visible }}
                    </span>
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
        </section>
      </main>

      <!-- Detail Modal -->
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
                  Queue: {{ service.selectedTaskDetail()!.queueName }}
                </p>
              </div>

              <button
                (click)="service.selectedTaskDetail.set(null)"
                class="text-slate-400 hover:text-slate-900 text-2xl font-bold">
                ✕
              </button>
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
  readonly service = inject(SqlFlowManagementService);

  readonly selectedView = signal<ViewId>('queue-states');
  readonly clientFilter = signal('');

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
      }
    ];
  });

  readonly currentTitle = computed(() => {
    const t = this.service.t();

    switch (this.selectedView()) {
      case 'queue-states':
        return t.queueStateDist;
      case 'workers':
        return t.activeWorkers;
      case 'backlog':
        return t.backlogDepth;
      case 'wait-times':
        return t.waitTimes;
      case 'throughput':
        return t.throughput;
      case 'latency':
        return t.latencyPercentiles;
      case 'failures':
        return t.failureHotspots;
      case 'failed-tasks':
        return t.failedTaskList;
      case 'slow-tasks':
        return t.slowTasks;
      case 'retries':
        return t.retryHotspots;
      case 'blockades':
        return t.eventBlockades;
      case 'search':
        return t.inspectorTab;
    }
  });

  readonly currentColumns = computed<EnterpriseColumn[]>(() => {
    const t = this.service.t();

    switch (this.selectedView()) {
      case 'queue-states':
        return [
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'state', label: t.state, kind: 'badge', value: x => x.state },
          { key: 'count', label: t.count, kind: 'number', align: 'center', value: x => x.count }
        ];

      case 'workers':
        return [
          { key: 'workerId', label: t.workerId, value: x => x.workerId },
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'activeRuns', label: t.runs, kind: 'number', align: 'center', value: x => x.activeRuns }
        ];

      case 'backlog':
        return [
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'oldestPendingAt', label: t.oldest, kind: 'date', value: x => this.formatDateTime(x.oldestPendingAt) },
          { key: 'pendingCount', label: t.pending, kind: 'number', align: 'center', value: x => x.pendingCount }
        ];

      case 'wait-times':
        return [
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'avgWaitTimeMs', label: t.avgWait, kind: 'number', align: 'center', value: x => Math.round(x.avgWaitTimeMs) },
          { key: 'maxWaitTimeMs', label: t.maxWait, kind: 'number', align: 'center', value: x => Math.round(x.maxWaitTimeMs) }
        ];

      case 'throughput':
        return [
          { key: 'timeBucket', label: t.timeBucket, kind: 'date', value: x => this.formatDateTime(x.timeBucket) },
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'completedCount', label: t.completedCount, kind: 'number', align: 'center', value: x => x.completedCount },
          { key: 'failedCount', label: t.failedCount, kind: 'number', align: 'center', value: x => x.failedCount },
          { key: 'avgDurationMs', label: t.avgDuration, kind: 'number', align: 'center', value: x => Math.round(x.avgDurationMs) }
        ];

      case 'latency':
        return [
          { key: 'taskName', label: t.taskName, value: x => x.taskName },
          { key: 'p50Ms', label: 'P50', kind: 'number', align: 'center', value: x => Math.round(x.p50Ms) },
          { key: 'p95Ms', label: 'P95', kind: 'number', align: 'center', value: x => Math.round(x.p95Ms) },
          { key: 'p99Ms', label: 'P99', kind: 'number', align: 'center', value: x => Math.round(x.p99Ms) }
        ];

      case 'failures':
        return [
          { key: 'taskName', label: t.taskName, value: x => x.taskName },
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'failureCount', label: t.fails, kind: 'number', align: 'center', value: x => x.failureCount },
          { key: 'lastFailedAt', label: t.lastFailed, kind: 'date', value: x => this.formatDateTime(x.lastFailedAt) }
        ];

      case 'failed-tasks':
        return [
          { key: 'taskId', label: t.taskId, value: x => this.shortId(x.taskId) },
          { key: 'taskName', label: t.taskName, value: x => x.taskName },
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'attempts', label: t.attempts, kind: 'number', align: 'center', value: x => x.attempts },
          { key: 'runId', label: 'Run ID', value: x => this.shortId(x.runId) },
          { key: 'failedAt', label: t.failedAt, kind: 'date', value: x => this.formatDateTime(x.failedAt) },
          { key: 'failureReason', label: t.failureReason, value: x => x.failureReason },
          { key: 'action', label: t.inspect, kind: 'action', align: 'center', value: () => t.viewJson }
        ];

      case 'slow-tasks':
        return [
          { key: 'taskId', label: t.taskId, value: x => this.shortId(x.taskId) },
          { key: 'taskName', label: t.taskName, value: x => x.taskName },
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'durationMs', label: t.duration, kind: 'number', align: 'center', value: x => Math.round(x.durationMs) },
          { key: 'completedAt', label: t.completedAt, kind: 'date', value: x => this.formatDateTime(x.completedAt) },
          { key: 'action', label: t.inspect, kind: 'action', align: 'center', value: () => t.viewJson }
        ];

      case 'retries':
        return [
          { key: 'taskName', label: t.taskName, value: x => x.taskName },
          { key: 'taskId', label: t.taskId, value: x => this.shortId(x.taskId) },
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'attempts', label: t.retries, kind: 'number', align: 'center', value: x => x.attempts },
          { key: 'state', label: t.state, kind: 'badge', value: x => x.state },
          { key: 'action', label: t.inspect, kind: 'action', align: 'center', value: () => t.viewJson }
        ];

      case 'blockades':
        return [
          { key: 'eventName', label: t.eventName, value: x => x.eventName },
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'waitingCount', label: t.blocked, kind: 'number', align: 'center', value: x => x.waitingCount },
          { key: 'oldestWaitAt', label: t.oldest, kind: 'date', value: x => this.formatDateTime(x.oldestWaitAt) }
        ];

      case 'search':
        return [
          { key: 'taskId', label: t.taskId, value: x => this.shortId(x.taskId) },
          { key: 'taskName', label: t.taskName, value: x => x.taskName },
          { key: 'queueName', label: t.queue, value: x => x.queueName },
          { key: 'state', label: t.state, kind: 'badge', value: x => x.state },
          { key: 'attempts', label: t.attempts, kind: 'number', align: 'center', value: x => x.attempts },
          { key: 'enqueuedAt', label: t.enqueuedAt, kind: 'date', value: x => this.formatDateTime(x.enqueuedAt) },
          { key: 'failureReason', label: t.failureReason, value: x => x.failureReason || x.paramsJson },
          { key: 'action', label: t.inspect, kind: 'action', align: 'center', value: () => t.viewJson }
        ];
    }
  });

  readonly currentRows = computed<any[]>(() => {
    const filter = this.clientFilter().trim().toLowerCase();

    const contains = (...values: Array<string | number | null | undefined>) => {
      if (!filter || this.selectedView() === 'search') {
        return true;
      }

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
        return this.service.throughput().filter(x =>
          contains(x.timeBucket, x.queueName, x.completedCount, x.failedCount, x.avgDurationMs)
        );

      case 'latency':
        return this.service.percentiles().filter(x => contains(x.taskName, x.p50Ms, x.p95Ms, x.p99Ms));

      case 'failures':
        return this.service.hotspots().filter(x => contains(x.taskName, x.queueName, x.failureCount, x.lastFailedAt));

      case 'failed-tasks':
        return this.service.failedTasks().filter(x =>
          contains(x.taskId, x.taskName, x.queueName, x.attempts, x.runId, x.failedAt, x.failureReason)
        );

      case 'slow-tasks':
        return this.service.slowTasks().filter(x =>
          contains(x.taskId, x.taskName, x.queueName, x.durationMs, x.completedAt)
        );

      case 'retries':
        return this.service.retryHotspots().filter(x =>
          contains(x.taskName, x.taskId, x.queueName, x.state, x.attempts)
        );

      case 'blockades':
        return this.service.activeWaits().filter(x =>
          contains(x.eventName, x.queueName, x.waitingCount, x.oldestWaitAt)
        );

      case 'search':
        return this.service.searchResults();
    }
  });

  readonly totalRows = computed(() => this.currentRows().length);

  ngOnInit(): void {
    this.refreshCurrentView();

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

  onRowAction(row: any): void {
    if (!row?.queueName || !row?.taskId) {
      return;
    }

    this.service.fetchTaskDetails(row.queueName, row.taskId);
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
        return 'Keine Wartezeiten vorhanden.';

      case 'throughput':
        return 'Keine Durchsatzdaten vorhanden.';

      case 'latency':
        return t.noLatencyData;

      case 'failures':
        return t.noHotspots;

      case 'failed-tasks':
        return 'Keine fehlgeschlagenen Tasks vorhanden.';

      case 'slow-tasks':
        return 'Keine langsamen Tasks vorhanden.';

      case 'retries':
        return t.noRetryHotspots;

      case 'blockades':
        return t.noBlockades;

      case 'search':
        return t.noMatchingTasks;
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
      default:
        return 'bg-slate-50 text-slate-700 border-slate-200';
    }
  }

  shortId(value: string | null | undefined): string {
    if (!value) {
      return '-';
    }

    return value.length > 12
      ? value.slice(0, 12)
      : value;
  }

  formatDateTime(value: string | null | undefined): string {
    if (!value) {
      return '-';
    }

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
