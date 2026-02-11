/**
 * Configuration Module
 * Centralized configuration with Zod validation
 */

import { z } from 'zod';
import dotenv from 'dotenv';
import { logger } from './logger.js';

// Load environment variables
dotenv.config();

/**
 * Environment variable schema
 */
const envSchema = z.object({
    // Metabase Configuration
    METABASE_URL: z.string().url('METABASE_URL must be a valid URL'),
    METABASE_USERNAME: z.string().email().optional(),
    METABASE_PASSWORD: z.string().min(1).optional(),
    METABASE_API_KEY: z.string().optional(),

    // Security Settings
    METABASE_READ_ONLY_MODE: z
        .string()
        .default('true')
        .transform(val => val.toLowerCase() === 'true'),

    // Internal Database ID (for advanced metadata features)
    METABASE_INTERNAL_DB_ID: z.string().optional().transform(val => val ? parseInt(val, 10) : undefined),

    // Database Configuration
    DATABASE_TYPE: z.enum(['postgres', 'mysql', 'sqlite']).default('postgres'),
    DATABASE_HOST: z.string().optional(),
    DATABASE_PORT: z.string().optional().transform(val => val ? parseInt(val, 10) : undefined),
    DATABASE_NAME: z.string().optional(),
    DATABASE_USER: z.string().optional(),
    DATABASE_PASSWORD: z.string().optional(),

    // Metadata Configuration
    MB_METADATA_ENABLED: z
        .string()
        .default('false')
        .transform(val => val.toLowerCase() === 'true'),

    // AI Configuration
    ANTHROPIC_API_KEY: z.string().optional(),
    OPENAI_API_KEY: z.string().optional(),
    AI_PROVIDER: z.enum(['anthropic', 'openai']).optional(),

    // Application Settings
    PORT: z.string().default('3001').transform(val => parseInt(val, 10)),
    LOG_LEVEL: z.enum(['debug', 'info', 'warn', 'error']).default('info'),
    ENVIRONMENT: z.enum(['development', 'production', 'test']).default('development'),

    // Cache Settings
    CACHE_TTL_MS: z
        .string()
        .default('600000')
        .transform(val => parseInt(val, 10)),
}).refine(
    data => data.METABASE_API_KEY || (data.METABASE_USERNAME && data.METABASE_PASSWORD),
    {
        message: 'Either METABASE_API_KEY or both METABASE_USERNAME and METABASE_PASSWORD must be provided',
        path: ['METABASE_API_KEY'],
    }
);

/**
 * Validate and parse environment variables
 * @returns {object} Validated configuration
 */
function validateConfig() {
    try {
        const config = envSchema.parse(process.env);
        logger.info('Configuration validated successfully');
        return config;
    } catch (error) {
        if (error instanceof z.ZodError) {
            const errorMessages = error.errors.map(err => `${err.path.join('.')}: ${err.message}`);
            logger.error('Environment validation failed:', errorMessages);

            // In development, log but continue with defaults
            if (process.env.ENVIRONMENT === 'development' || process.env.NODE_ENV === 'development') {
                logger.warn('Running with partial configuration in development mode');
                return envSchema.partial().parse(process.env);
            }

            throw new Error(`Environment validation failed:\n${errorMessages.join('\n')}`);
        }
        throw error;
    }
}

/**
 * Create safe configuration (without sensitive data)
 * @returns {object}
 */
function createSafeConfig() {
    return {
        metabaseUrl: process.env.METABASE_URL,
        readOnlyMode: process.env.METABASE_READ_ONLY_MODE !== 'false',
        metadataEnabled: process.env.MB_METADATA_ENABLED === 'true',
        aiProvider: process.env.ANTHROPIC_API_KEY ? 'anthropic' : (process.env.OPENAI_API_KEY ? 'openai' : null),
        environment: process.env.ENVIRONMENT || 'development',
        logLevel: process.env.LOG_LEVEL || 'info',
        cacheTTL: parseInt(process.env.CACHE_TTL_MS) || 600000,
    };
}

// Export validated config
let config;
try {
    config = validateConfig();
} catch (error) {
    // Fallback for non-strict mode
    config = createSafeConfig();
}

export { config, validateConfig, createSafeConfig };
export default config;
