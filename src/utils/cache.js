/**
 * Cache Utility Module
 * TTL-based caching system for Metabase API responses
 */

import { logger } from './logger.js';

/**
 * Cache Manager class for managing API response caching
 */
export class CacheManager {
    /**
     * @param {object} options
     * @param {number} options.ttl - Time to live in milliseconds (default: 10 minutes)
     */
    constructor(options = {}) {
        this.ttl = options.ttl || 600000; // 10 minutes default
        this.maxSize = options.maxSize || 500; // Max entries
        this.cache = new Map();
        this.stats = {
            hits: 0,
            misses: 0,
            sets: 0,
            clears: 0,
            evictions: 0
        };
    }

    /**
     * Get a value from cache
     * @param {string} key - Cache key
     * @returns {any|null} Cached value or null if expired/missing
     */
    get(key) {
        const item = this.cache.get(key);

        if (!item) {
            this.stats.misses++;
            return null;
        }

        // Check if expired
        if (Date.now() - item.timestamp > this.ttl) {
            this.cache.delete(key);
            this.stats.misses++;
            logger.debug(`Cache miss (expired): ${key}`);
            return null;
        }

        this.stats.hits++;
        logger.debug(`Cache hit: ${key}`);
        return item.data;
    }

    /**
     * Set a value in cache
     * @param {string} key - Cache key
     * @param {any} data - Data to cache
     */
    set(key, data) {
        // Evict oldest entries if at capacity
        if (this.cache.size >= this.maxSize && !this.cache.has(key)) {
            const oldestKey = this.cache.keys().next().value;
            this.cache.delete(oldestKey);
            this.stats.evictions++;
            logger.debug(`Cache evicted (maxSize): ${oldestKey}`);
        }
        this.cache.set(key, {
            data,
            timestamp: Date.now()
        });
        this.stats.sets++;
        logger.debug(`Cache set: ${key}`);
    }

    /**
     * Get or set cache value (with fetch function)
     * @param {string} key - Cache key
     * @param {Function} fetchFn - Async function to fetch data if not cached
     * @returns {Promise<{data: any, source: string, fetchTime: number}>}
     */
    async getOrSet(key, fetchFn) {
        const cached = this.get(key);

        if (cached !== null) {
            return {
                data: cached,
                source: 'cache',
                fetchTime: 0
            };
        }

        const startTime = Date.now();
        const data = await fetchFn();
        const fetchTime = Date.now() - startTime;

        this.set(key, data);

        return {
            data,
            source: 'api',
            fetchTime
        };
    }

    /**
     * Clear a specific cache key
     * @param {string} key - Cache key to clear
     */
    clear(key) {
        this.cache.delete(key);
        this.stats.clears++;
        logger.debug(`Cache cleared: ${key}`);
    }

    /**
     * Clear all cache entries matching a pattern
     * @param {string} pattern - Pattern to match (prefix)
     */
    clearByPattern(pattern) {
        let cleared = 0;
        for (const key of this.cache.keys()) {
            if (key.startsWith(pattern)) {
                this.cache.delete(key);
                cleared++;
            }
        }
        this.stats.clears += cleared;
        logger.debug(`Cache cleared by pattern "${pattern}": ${cleared} entries`);
    }

    /**
     * Clear all cache entries
     */
    clearAll() {
        const size = this.cache.size;
        this.cache.clear();
        this.stats.clears += size;
        logger.info('All cache cleared');
    }

    /**
     * Get cache statistics
     * @returns {object}
     */
    getStats() {
        const total = this.stats.hits + this.stats.misses;
        return {
            ...this.stats,
            size: this.cache.size,
            maxSize: this.maxSize,
            hitRate: total > 0 ? (this.stats.hits / total * 100).toFixed(2) + '%' : 'N/A'
        };
    }
}

// Cache key generators
export const CacheKeys = {
    databases: () => 'databases',
    database: (id) => `database:${id}`,
    databaseSchemas: (id) => `database:${id}:schemas`,
    databaseTables: (id) => `database:${id}:tables`,
    table: (id) => `table:${id}`,
    tableFields: (id) => `table:${id}:fields`,
    dashboards: () => 'dashboards',
    dashboard: (id) => `dashboard:${id}`,
    questions: (collectionId) => collectionId ? `questions:${collectionId}` : 'questions',
    question: (id) => `question:${id}`,
    collections: () => 'collections',
    collection: (id) => `collection:${id}`,
};

// Singleton instance for global use
export const globalCache = new CacheManager();

export default CacheManager;
