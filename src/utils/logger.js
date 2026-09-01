import winston from 'winston';
import chalk from 'chalk';

const { combine, timestamp, printf, colorize } = winston.format;

// Custom format for console output
const consoleFormat = printf(({ level, message, timestamp, ...meta }) => {
  let msg = `${timestamp} [${level}]: ${message}`;

  if (Object.keys(meta).length > 0) {
    msg += ` ${JSON.stringify(meta)}`;
  }

  return msg;
});

// Create logger instance
export const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: combine(
    timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [
    // Stream transport to stderr - preserves pure JSON-RPC on stdout
    new winston.transports.Stream({
      stream: process.stderr,
      format: combine(
        colorize(),
        timestamp({ format: 'HH:mm:ss' }),
        consoleFormat
      )
    })
  ]
});

// Add console methods for colored output routed to stderr
export const console = {
  log: (msg) => process.stderr.write(chalk.white(msg) + '\n'),
  info: (msg) => process.stderr.write(chalk.cyan(msg) + '\n'),
  success: (msg) => process.stderr.write(chalk.green(msg) + '\n'),
  warning: (msg) => process.stderr.write(chalk.yellow(msg) + '\n'),
  error: (msg) => process.stderr.write(chalk.red(msg) + '\n')
};