import { randomUUID } from 'crypto';
import { logger } from '../utils/logger.js';

/**
 * QueryJobStore - Manages async query jobs
 * Tracks job status, results, and handles cleanup
 */
export class QueryJobStore {
    constructor() {
        this.jobs = new Map();

        // Auto-cleanup every 10 minutes
        this.cleanupInterval = setInterval(() => this.cleanup(), 10 * 60 * 1000);
    }

    /**
     * Create a new query job
     */
    create(databaseId, sql, timeoutSeconds = 300) {
        const jobId = randomUUID();
        const job = {
            id: jobId,
            database_id: databaseId,
            sql: sql,
            status: 'pending', // pending | running | complete | failed | timeout | cancelled
            submitted_at: Date.now(),
            started_at: null,
            completed_at: null,
            timeout_ms: timeoutSeconds * 1000,
            result: null,
            error: null,
            row_count: 0,
            abortController: new AbortController()
        };

        this.jobs.set(jobId, job);
        logger.info(`Query job created: ${jobId}`);
        return job;
    }

    /**
     * Get job by ID
     */
    get(jobId) {
        return this.jobs.get(jobId);
    }

    /**
     * Update job properties
     */
    update(jobId, updates) {
        const job = this.jobs.get(jobId);
        if (job) {
            Object.assign(job, updates);
            logger.debug(`Job ${jobId} updated: ${updates.status || 'props'}`);
        }
        return job;
    }

    /**
     * Mark job as running
     */
    markRunning(jobId) {
        return this.update(jobId, {
            status: 'running',
            started_at: Date.now()
        });
    }

    /**
     * Mark job as complete with results
     */
    markComplete(jobId, result, rowCount) {
        return this.update(jobId, {
            status: 'complete',
            completed_at: Date.now(),
            result: result,
            row_count: rowCount
        });
    }

    /**
     * Mark job as failed
     */
    markFailed(jobId, error) {
        return this.update(jobId, {
            status: 'failed',
            completed_at: Date.now(),
            error: error.message || String(error)
        });
    }

    /**
     * Mark job as timed out
     */
    markTimeout(jobId) {
        const job = this.jobs.get(jobId);
        return this.update(jobId, {
            status: 'timeout',
            completed_at: Date.now(),
            error: `Query timed out after ${job?.timeout_ms / 1000} seconds`
        });
    }

    /**
     * Mark job as cancelled
     */
    markCancelled(jobId) {
        return this.update(jobId, {
            status: 'cancelled',
            completed_at: Date.now(),
            error: 'Query cancelled by user'
        });
    }

    /**
     * Get elapsed time for a job in seconds
     */
    getElapsedSeconds(jobId) {
        const job = this.jobs.get(jobId);
        if (!job) return 0;

        const startTime = job.started_at || job.submitted_at;
        const endTime = job.completed_at || Date.now();
        return Math.round((endTime - startTime) / 1000);
    }

    /**
     * List all jobs (optionally filtered by status)
     */
    list(status = null) {
        const jobs = [];
        for (const [id, job] of this.jobs) {
            if (!status || job.status === status) {
                jobs.push({
                    id: job.id,
                    database_id: job.database_id,
                    status: job.status,
                    submitted_at: job.submitted_at,
                    elapsed_seconds: this.getElapsedSeconds(job.id)
                });
            }
        }
        return jobs;
    }

    /**
     * Get running jobs count
     */
    getRunningCount() {
        let count = 0;
        for (const job of this.jobs.values()) {
            if (job.status === 'running' || job.status === 'pending') {
                count++;
            }
        }
        return count;
    }

    /**
     * Cleanup old completed jobs (older than 1 hour)
     */
    cleanup() {
        const oneHourAgo = Date.now() - 3600000;
        let cleaned = 0;

        for (const [id, job] of this.jobs) {
            if (job.status !== 'running' && job.status !== 'pending') {
                if (job.submitted_at < oneHourAgo) {
                    this.jobs.delete(id);
                    cleaned++;
                }
            }
        }

        if (cleaned > 0) {
            logger.info(`Cleaned up ${cleaned} old query jobs`);
        }
    }

    /**
     * Stop the cleanup interval (for graceful shutdown)
     */
    destroy() {
        if (this.cleanupInterval) {
            clearInterval(this.cleanupInterval);
        }
    }
}

// Singleton instance
let instance = null;

export function getJobStore() {
    if (!instance) {
        instance = new QueryJobStore();
    }
    return instance;
}
