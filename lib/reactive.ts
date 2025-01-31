import { cloneDeepWith, debounce, isPlainObject } from 'lodash-es';
import { DebouncedFunc, DebounceSettings } from 'lodash-es';
import { nextTick, reactive as VueReactive, watch as VueWatch, WatchStopHandle, WatchOptions, WatchCallback, WatchSource, Reactive } from 'vue';
import { SupabaseClient, REALTIME_LISTEN_TYPES, RealtimePostgresChangesFilter, REALTIME_POSTGRES_CHANGES_LISTEN_EVENT } from '@supabase/supabase-js';
import { PostgrestFilterBuilder } from '@supabase/postgrest-js';

interface SupabaseReactiveOptions {
  supabase: SupabaseClient;
  table?: string;
  id?: string;
  isArray?: boolean;
  read?: boolean;
  watch?: boolean;
  write?: boolean;
  attachReactives?: boolean;
  throttle?: DebounceSettings & { wait: number };
  idColumn?: string;
  filter?: [string, string, unknown]; // [column, operator, value] - Assuming PostgrestFilterBuilder.filter() parameters
  dataColumn?: string;
  timestampColumn?: string;
  versionColumn?: string;
  reactiveCreate?: <T extends object>(state: T) => Reactive<T>;
  reactiveWatch?: <T>(
    target: WatchSource<T>,
    cb: WatchCallback<T, T>,
    options?: WatchOptions
  ) => WatchStopHandle;
  onInit?: (data: object | unknown[]) => Promise<void> | void;
  onRead?: (data: object | unknown[]) => Promise<void> | void;
  onChange?: (dataPayload: object | unknown[]) => Promise<void> | void;
  onDestroy?: (data: object) => Promise<void> | void;
  debug?: ((...msg: unknown[]) => void) | boolean;
  splitPath?: (path: string, settings: SupabaseReactiveOptions) => void;
}

interface ReactiveMeta {
  id: string | null;
  table: string | null;
  timestamp: Date | null;
  version: number | null;
  isUpdating: boolean;
  watcher: WatchStopHandle | null;
  setQueue: SetQueueItem[];
}

interface SetQueueItem {
  data: object | unknown[];
  options: SetOptions;
  promise: Promise<void> | null;
  resolve: () => void;
  reject: () => void;
}

interface SetOptions {
  markUpdating?: boolean;
  updateDelay?: number;
  removeKeys?: boolean;
  timestamp?: Date | null;
  version?: number | null;
  queue?: boolean;
  queueDrain?: boolean;
}

interface TouchLocalWithPromise {
  (): Promise<void>;
  promise?: PromiseLike<boolean>;
}

interface SubscribeWithIsSubscribed {
  (isSubscribed?: boolean): Promise<void>;
  isSubscribed?: boolean;
}

interface Reactives<T extends object> {
  $meta: ReactiveMeta;
  $waitTick: (delay: number) => Promise<void>;
  $set: (data: T, options?: SetOptions) => Promise<void>;
  $toObject: () => T;
  $refresh: (options?: object) => Promise<void>;
  $getQuery: () => PostgrestFilterBuilder<any, any, any>;
  $init: () => Promise<Reactive<T>>;
  $read: (options?: { force?: boolean }) => Promise<void>;
  $fetch: () => Promise<object | unknown[]>;
  $watch: (isWatching?: boolean) => Promise<void>;
  $touchLocal: TouchLocalWithPromise;
  $touchRemote: (data: { new: any }) => Promise<void>;
  $tick: (delay?: number) => Promise<void>;
  $flush: (delay?: number) => Promise<[boolean | undefined, void]>;
  $subscribe: SubscribeWithIsSubscribed;
  $destroy: () => Promise<void>;
}

const defaults: SupabaseReactiveOptions = {
  // Reactive instance
  supabase: null!, // Will be overridden
  table: undefined,
  id: undefined,
  isArray: false,

  // Reactive options
  read: true,
  watch: true,
  write: true,
  attachReactives: true,
  throttle: {
    wait: 200,
    maxWait: 2000,
    leading: false,
    trailing: true,
  },

  // Table structure
  idColumn: 'id',
  filter: undefined,
  dataColumn: 'data',
  timestampColumn: 'edited_at',
  versionColumn: undefined,

  // Reactive control
  reactiveCreate(state) {
    return VueReactive(state);
  },
  reactiveWatch<T>(target, cb, options) {
    return VueWatch(target, cb as WatchCallback<WatchSource<T>, WatchSource<T> | undefined>, {
      deep: true,
      ...options,
    });
  },

  // Callbacks
  onInit(data) {},
  onRead(data) {},
  onChange(data) {},
  onDestroy(data) {},

  // Utilities
  debug: undefined, // Init in settings setup, false to disable
  splitPath(value, settings) {
    // Split paths of the form 'TABLE/ID' into their options
    const pathValues = /^\/?(?<table>[\w_\-]+?)\/(?<id>.+)$/.exec(value)?.groups;
    if (!pathValues) throw new Error(`Unable to decode path "${value}"`);
    Object.assign(settings, pathValues);
  },
};

/**
 * Return a reactive object (or array) which syncs local and remote state
 *
 * @param {String} [path] Optional, shorthand path to subscribe to. By default this is of the form `$TABLE/$ID`
 *
 * @param {Object} [options] Additional options to configure behaviour, uses this modules defaults first if unspecified
 *
 * @returns {Promise<Reactive>} An eventual reactive Object/Array with utility functions (if `{attachReactives:true}`)
 */
function SupabaseReactive<T extends object>(
  path: string | SupabaseReactiveOptions,
  options?: SupabaseReactiveOptions
): Promise<T & Partial<Reactives<T>>> {
  const settings: SupabaseReactiveOptions = {
    ...defaults,
    ...(typeof path === 'object' ? path : options),
  };

  // Settings init
  if (!settings.supabase) throw new Error('No `supabase` setting given');
  if (!settings.reactiveCreate) throw new Error('No `reactiveCreate` setting given')

  if (typeof path === 'string' && settings.splitPath) settings.splitPath(path, settings);

  settings.debug =
    settings.debug && typeof settings.debug === 'function'
      ? settings.debug.bind(settings) // Given a debug function
      : settings.debug === false
      ? () => {}
      : console.log.bind(settings, `[SUPABASE/${settings.table}/${settings.id}]`);

  const reactive = settings.reactiveCreate<T>(
    !settings.isArray ? ({} as T) : ([] as unknown as T)
  );

  /**
   * Base reactive functionality mapped onto the output as non-enumerable functions
   * These are Functions appended to the binding which can be called to perform various utility actions
   */
  const reactives: Reactives<T> = {
    /**
     * Meta information about the current row
     * (This only really exists because we can't assign scalars in Javascript without it resetting the pointer later)
     */
    $meta: settings.reactiveCreate<ReactiveMeta>({
      id: settings.id === undefined ? null : settings.id,
      table: settings.table === undefined ? null : settings.table,
      timestamp: null,
      version: null,
      isUpdating: false,
      watcher: null,
      setQueue: [],
    }),

    /**
     * Wait for Vue to update + a set amount of time to expire
     * This is used within $set() to correctly release the write lock
     *
     * @param {Number} delay Time in milliseconds to wait alongside Vue.$nextTick
     * @returns {Promise} A promise which will resolve when both Vue has moved on a tick + a set timeout has occured
     */
    async $waitTick(delay) {
      await nextTick();
      await new Promise<void>((resolve) => setTimeout(() => resolve(), delay));
    },

    /**
     * Set the content of the reactive
     *
     * @param {Object|Array} data New state to adopt
     *
     * @param {Object} [options] Additional options to mutate behaviour
     * @returns {Promise} A promise which resolves when the operation has completed
     */
    async $set(data, options) {
      options = {
        markUpdating: true,
        updateDelay: 1000,
        removeKeys: true,
        timestamp: null,
        version: null,
        queue: true,
        queueDrain: true,
        ...options,
      };

      // Handle write collisions {{{
      if (options.markUpdating) {
        if (reactives.$meta.isUpdating) {
          if (options.queue) {
            // Allowed to queue up writes
            const queueItem: SetQueueItem = {
              data,
              options,
              promise: null, // Deferred promise created below
              resolve: () => {}, // Computed when promise defer finishes
              reject: () => {},
            };
            queueItem.promise = new Promise((resolve, reject) => {
              Object.assign(queueItem, { resolve, reject });
            });
            reactives.$meta.setQueue.push(queueItem);
            (settings.debug as Function)('$set already in progress - queuing mutation');
            await queueItem.promise; // Wait for the pending promise
          } else {
            throw new Error('Reactive.$set() already in process! Recursion prevented');
          }
        }
        reactives.$meta.isUpdating = true;
      }
      // }}}

      // Apply the data change
      Object.assign(reactive, data);

      // options.removeKeys? {{{
      if (options.removeKeys) {
        if (!(typeof data === 'object' && data !== null)) {
          (settings.debug as Function)(`Skipping removing keys, data is not an object: ${data}`)

        } else {
          Object.keys(reactive).forEach((key) => {
            if (!(key in data)) {
              (settings.debug as Function)('Remove redundent key', key);
              delete reactive[key];
            }
          });
        }
      }
      // }}}

      // Append timestamps and/or version {{{
      if (options.timestamp) reactives.$meta.timestamp = options.timestamp;
      if (options.version) reactives.$meta.version = options.version;
      // }}}

      // Releasing the update lock - this has to be after the next update cycle so we don't get trapped in a $watch->change loop {{{
      if (options.markUpdating) {
        await reactives.$waitTick(options.updateDelay ? options.updateDelay : 1000);

        (settings.debug as Function)('Release write lock');
        reactives.$meta.isUpdating = false;
      }
      // }}}

      // $meta.setQueue draining {{{
      if (options.queueDrain && reactives.$meta.setQueue.length > 0) {
        // Drain the remaining queue
        const lastSet = reactives.$meta.setQueue.pop()!; // Take the last set item
        const remainingQueue = reactives.$meta.setQueue;
        reactives.$meta.setQueue = []; // Reset the queue

        if (remainingQueue.length > 0) {
          (settings.debug as Function)('Dispose of', remainingQueue.length, 'stale states');
          await Promise.all(remainingQueue.map((sq) => sq.resolve())); // Resolve all pending promises - we can ignore the payload on all these items as only the last one is relevent anyway
        }
        await reactives.$set(lastSet.data as T, { ...options, ...lastSet.options }); // Execute the last state change against $set
        await lastSet.resolve(); // Fire the remaining $set resolver
      }
      // }}}
    },

    /**
     * Tidy JSON field data so that is safe from private methods (anything starting with '$' or '_', proxies or other non POJO slush
     *
     * @returns {Object|Array} POJO, scalar output
     */
    $toObject() {
      return cloneDeepWith(reactive, (v, k) =>
        // Clone so we break up all the proxy slush and store primatives only
        !/^[$\_]/.test(k as string) && // Key doesn't start with '$' or '_'
        (['string', 'number', 'boolean'].includes(typeof v) ||
          Array.isArray(v) ||
          isPlainObject(v))
          ? undefined // Use default cloning behaviour
          : null // Strip from output
      );
    },

    /**
     * Alias of `$read()`
     * @alias $read
     * @param {Object} [options] Additional options to mutate behaviour
     *
     * @returns {Promise} A promise which resolves when the operation has completed
     */
    $refresh(options) {
      return reactives.$read(options);
    },

    /**
     * Generate a Supabase object representing a query for the current configuration
     *
     * @returns {Promise} A Supabase promise which resolves when the operation has completed
     */
    $getQuery() {
      let query: PostgrestFilterBuilder<any, any, any> = settings.supabase
        .from(settings.table!)
        .select(
          [
            settings.idColumn,
            settings.timestampColumn,
            settings.dataColumn,
            settings.versionColumn && settings.versionColumn,
          ]
            .filter(Boolean)
            .join(',')
        );

      if (settings.isArray || settings.filter) {
        query = query.filter(...settings.filter!);
      } else {
        query = query.eq(settings.idColumn!, settings.id!);
      }

      // TODO: Investigate if this any casting is valid
      if (!settings.isArray) query = query.single() as any;

      return query;
    },

    /**
     * Initial operaton to wait on data from service + return reactable
     * This function is the default response when calling the outer `Reactive()` function
     *
     * @returns {Promise<Reactive<Object>>} A promise which resolves with the initial data state when loaded
     */
    async $init() {
      if (reactives.$meta.timestamp)
        throw new Error('Reactive.$init() has already been called');

      // Read initial state (if settings.read)
      if (settings.read) await reactives.$read();

      // Subscribe to local watcher (if settings.watch)
      if (settings.watch) await reactives.$watch();

      // Subscribe to remote (if settings.write)
      if (settings.write) await reactives.$subscribe();

      return reactive;
    },

    /**
     * Fetch the current data state from the server and update the reactive
     *
     * @param {Object} [options] Additional options to mutate behaviour
     * @param {Boolean} [options.force=false] Forcibly read in server values, overriding local values
     *
     * @returns {Promise} A promise which resolves when the operation has completed
     */
    async $read(options) {
      const readSettings = {
        force: false,
        ...options,
      };

      const { data } = await reactives.$getQuery();

      // Mangle incoming row into a dataVal
      const dataVal = settings.isArray
        ? (data as any[]).map((row) => ({
            id: row.id,
            ...row[settings.dataColumn!],
          }))
        : data?.[settings.dataColumn!] || {};

      // Mangle incoming row into a dataTimestamp
      const dataTimestamp = settings.isArray
        ? (data as any[]).reduce(
            (latest, row) =>
              latest === null || latest < row[settings.timestampColumn!]
                ? row[settings.timestampColumn!]
                : latest,
            null
          )
        : data?.[settings.timestampColumn!];

      // Mangle incoming row into a dataVersion
      const dataVersion = !settings.versionColumn
        ? null
        : settings.isArray
        ? (data as any[]).reduce(
            (largest, row) =>
              largest === null || largest < row[settings.versionColumn!]
                ? row[settings.versionColumn!]
                : largest,
            null
          )
        : data?.[settings.versionColumn!];

      // Trigger callbacks if its an init or simple read operation
      if (reactives.$meta.version === null) {
        (settings.debug as Function)('INIT VALUE', dataVal);
        reactives.$meta.version = 0;
        await settings.onInit!(dataVal);
      } else {
        (settings.debug as Function)('READ VALUE', dataVal);
        await settings.onRead!(dataVal);
      }

      // Assign the data
      await reactives.$set(dataVal as T, {
        timestamp: dataTimestamp,
        version: dataVersion,
        removeKeys: !readSettings.force,
      });
    },

    /**
     * Fetch the current data state from the server but don't update the local state
     * This function is only really useful for snapshotting server state
     *
     * @returns {Promise} A promise which resolves when the operation has completed
     */
    async $fetch() {
      const { data } = await reactives.$getQuery();

      return settings.isArray
        ? (data as any[]).map((row) => ({
            id: row.id,
            ...row[settings.dataColumn!],
          }))
        : data?.[settings.dataColumn!] || {};
    },

    /**
     * Watch local data for changes and push to the server as needed
     *
     * @param {Boolean} [isWatching=true] Status of the local watcher
     * @returns {Promise} A promise which resolves when the operation has completed
     */
    async $watch(isWatching = true) {
      if (isWatching == !!reactives.$meta.watcher) return; // Already in the state requested

      if (isWatching) {
        // Subscribe
        (settings.debug as Function)('Subscribed to local changes');
        reactives.$meta.watcher = settings.reactiveWatch!(
          // TODO: Investigate if these WatchSource and WatchCallback castings are valid
          reactive as WatchSource<any>,
          settings.throttle
            ? debounce(reactives.$touchLocal, settings.throttle.wait, settings.throttle) as WatchCallback<any, any>
            : reactives.$touchLocal as WatchCallback<any, any>
        );
      } else {
        // Unsubscribe
        (settings.debug as Function)('UN-subscribed from local changes');
        reactives.$meta.watcher!();
        reactives.$meta.watcher = null;
      }
    },

    /**
     * Internal function called when detecting a local change
     *
     * @access private
     * @returns {Promise} A promise which resolves when the operation has completed
     */
    async $touchLocal() {
      if (reactives.$meta.isUpdating) return; // Elsewhere is updating - ignore all local callbacks

      const payload = reactives.$toObject();
      const payloadTimestamp = new Date();
      const payloadVersion = settings.versionColumn
        ? (reactives.$meta.version ?? 0) + 1
        : 0;

      if (settings.isArray)
        throw new Error('TODO: Local array syncing is not yet supported');

      await settings.onChange!(payload);

      // Store local timestamp so we don't get into a loop when the server tells us about the change we're about to make
      reactives.$meta.timestamp = payloadTimestamp;

      // Increment local version (if defined else set to 1)
      if (settings.versionColumn) reactives.$meta.version ? reactives.$meta.version++ : reactives.$meta.version = 1;

      (settings.debug as Function)('LOCAL CHANGE', {
        $meta: reactives.$meta,
        payload,
      });

      // Assign a pending promise so calls to flush() can wait on this
      reactives.$touchLocal.promise = settings.supabase
        .from(settings.table!)
        .upsert(
          {
            [settings.idColumn!]: reactives.$meta.id,
            [settings.dataColumn!]: payload,
            [settings.timestampColumn!]: payloadTimestamp,
            ...(settings.versionColumn && {
              [settings.versionColumn]: payloadVersion,
            }),
          },
          {
            onConflict: settings.idColumn,
            ignoreDuplicates: false,
          }
        )
        .eq(settings.idColumn!, reactives.$meta.id)
        .select('id')
        .then(() =>
          (settings.debug as Function)('LOCAL CHANGE flushed', {
            newTimestamp: payloadTimestamp,
            newVersion: payloadVersion,
          })
        )
        .then(() => true); // FIX: Need to end on a promisable here otherwise Supabase can sometimes get confused and not execute the query

      await reactives.$touchLocal.promise;
    },

    /**
     * Internal function called when detecting a remote change
     *
     * @access private
     *
     * @param {Object} [data] New data payload to proocess
     *
     * @returns {Promise} A promise which resolves when the operation has completed
     */
    async $touchRemote(data) {
      if (!data.new) return; // No payload to prcess anyway

      // Tidy up incoming data fields
      const dataVersion = data.new[settings.versionColumn!];
      const dataTimestamp = new Date(data.new[settings.timestampColumn!]);
      const newData = data.new[settings.dataColumn!];

      (settings.debug as Function)(
        'REMOTE CHANGE',
        settings.versionColumn
          ? `Server@${dataVersion}, Local@${reactives.$meta.version}`
          : `Server@${
              dataTimestamp ? dataTimestamp.toISOString() : 'NOW'
            }, Local@${
              reactives.$meta.timestamp ? reactives.$meta.timestamp : '[NONE]'
            }`,
        newData
      );

      if (
        settings.versionColumn &&
        dataVersion <= reactives.$meta.version!
      )
        return (settings.debug as Function)('Reject server update - local version is recent enough', {
          localVersion: reactives.$meta.version,
          serverVersion: dataVersion,
        });
      if (
        reactives.$meta.timestamp &&
        dataTimestamp <= reactives.$meta.timestamp
      )
        return (settings.debug as Function)('Reject server update - local timestamp is recent enough', {
          localTimestamp: reactives.$meta.timestamp,
          serverTimestamp: dataTimestamp,
        });
      if (newData === undefined)
        return (settings.debug as Function)('Reject server update - newData is undefined', {
          localTimestamp: reactives.$meta.timestamp,
          serverTimestamp: dataTimestamp,
        });

      await reactives.$set(newData, {
        removeKeys: true,
        timestamp: dataTimestamp,
        ...(settings.versionColumn && {
          version: dataVersion,
        }),
      });

      await settings.onRead!(newData);
    },

    /**
     * Universal wrapper around setTimeout() which returns a promise
     * NOTE: We can't use node:timers/promises as this may be a front-end install
     *
     * @param {Number} [delay=0] The number of milliseconds to wait
     * @returns {Promise} A promise which resolves when the timeout has completed
     */
    async $tick(delay = 0) {
      return new Promise((resolve) => setTimeout(() => resolve(), delay));
    },

    /**
     * Wait for all local writes to complete
     * NOTE: This only promises that local writes complete, not that a subsequent read is required
     *
     * @param {Number} [delay=0] The number of milliseconds to wait for write operations to clear
     * @returns {Promise} A promise which resolves when the operation has completed
     */
    async $flush(delay = 100) {
      return Promise.all([
        reactives.$touchLocal.promise,
        reactives.$tick(delay),
      ]);
    },

    /**
     * Toggle subscription to the realtime datafeed
     *
     * @param {Boolean} [isSubscribed=true] Whether to enact the subscriptioon, set to false to remove subscriptions
     *
     * @returns {Promise} A promise which resolves when the operation has completed
     */
    async $subscribe(isSubscribed = true) {
      if (isSubscribed == reactives.$subscribe.isSubscribed) return; // Already in the state requested

      if (isSubscribed) {
        // Subscribe to remote
        (settings.debug as Function)('Subscribed to remote changes');
        const subscribeQuery: RealtimePostgresChangesFilter<`${REALTIME_POSTGRES_CHANGES_LISTEN_EVENT.UPDATE}`> = {
          event: 'UPDATE',
          schema: 'public',
          table: settings.table!,
          filter:
            settings.isArray || settings.filter
              ? settings.filter!.join('')
              : `${settings.idColumn}=eq.${settings.id}`,
        };

        settings.supabase.channel('any')
          .on(REALTIME_LISTEN_TYPES.POSTGRES_CHANGES, subscribeQuery, reactives.$touchRemote)
          .subscribe();
        reactives.$subscribe.isSubscribed = true;
      } else {
        // Unsubscribe from remote
        (settings.debug as Function)('UNsubscribed from remote changes');
        (settings.debug as Function)('TODO: Unsub from remote watchers is not yet supported');
        reactives.$subscribe.isSubscribed = false;
      }
    },

    /**
     * Release all watchers and subscriptions, local and remote
     *
     * @returns {Promise} A promise which resolves when the operation has completed
     */
    async $destroy() {
      await settings.onDestroy!(reactive);

      await Promise.all([reactives.$watch(false), reactives.$subscribe(false)]);
    },
  };

  if (settings.attachReactives && !settings.isArray) {
    Object.defineProperties(
      reactive,
      Object.fromEntries(
        Object.entries(reactives).map(([key, value]) => [
          key,
          {
            value,
            enumerable: false,
            configurable: false,
            writable: typeof value !== 'function',
          },
        ])
      )
    );
  }

  return reactives.$init() as Promise<T & Partial<Reactives<T>>>;
}

export default SupabaseReactive;