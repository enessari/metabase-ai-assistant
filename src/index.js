#!/usr/bin/env node

import dotenv from 'dotenv';
import chalk from 'chalk';
import { InteractiveCLI } from './cli/interactive.js';
import { MetabaseClient } from './metabase/client.js';
import { MetabaseAIAssistant } from './ai/assistant.js';
import { logger } from './utils/logger.js';

// Load environment variables
dotenv.config();

// Parse command line arguments
const args = process.argv.slice(2);
const command = args[0];

async function main() {
  try {
    // Check for required environment variables
    const requiredEnvVars = ['METABASE_URL'];
    const missingVars = requiredEnvVars.filter(v => !process.env[v]);
    
    if (missingVars.length > 0 && !process.env.METABASE_API_KEY) {
      console.error(chalk.red('Missing required environment variables:'));
      missingVars.forEach(v => console.error(chalk.red(`  - ${v}`)));
      console.error(chalk.yellow('\nPlease create a .env file based on .env.example'));
      process.exit(1);
    }

    // Handle different commands
    switch (command) {
      case 'test':
      case '--test':
        await testConnection();
        break;
      
      case 'help':
      case '--help':
      case '-h':
        showHelp();
        break;
      
      case 'version':
      case '--version':
      case '-v':
        showVersion();
        break;
      
      default:
        // Start interactive CLI
        await startInteractiveCLI();
        break;
    }
  } catch (error) {
    logger.error('Application error:', error);
    console.error(chalk.red('An error occurred:'), error.message);
    process.exit(1);
  }
}

async function startInteractiveCLI() {
  // Security: Never hardcode sensitive credentials
  if (!process.env.METABASE_URL || !process.env.METABASE_USERNAME || !process.env.METABASE_PASSWORD) {
    console.error(chalk.red('Required environment variables not set!'));
    console.error(chalk.yellow('Please set: METABASE_URL, METABASE_USERNAME, METABASE_PASSWORD'));
    console.error(chalk.yellow('Create a .env file based on .env.example'));
    process.exit(1);
  }

  const cli = new InteractiveCLI({
    metabaseUrl: process.env.METABASE_URL,
    metabaseUsername: process.env.METABASE_USERNAME,
    metabasePassword: process.env.METABASE_PASSWORD,
    metabaseApiKey: process.env.METABASE_API_KEY,
    aiProvider: process.env.AI_PROVIDER || 'anthropic',
    anthropicApiKey: process.env.ANTHROPIC_API_KEY,
    openaiApiKey: process.env.OPENAI_API_KEY
  });
  
  await cli.start();
}

async function testConnection() {
  console.log(chalk.cyan('Testing Metabase connection...'));
  
  // Security: Never hardcode sensitive credentials
  if (!process.env.METABASE_URL || !process.env.METABASE_USERNAME || !process.env.METABASE_PASSWORD) {
    console.error(chalk.red('Required environment variables not set!'));
    console.error(chalk.yellow('Please set: METABASE_URL, METABASE_USERNAME, METABASE_PASSWORD'));
    process.exit(1);
  }
  
  const client = new MetabaseClient({
    url: process.env.METABASE_URL,
    username: process.env.METABASE_USERNAME,
    password: process.env.METABASE_PASSWORD,
    apiKey: process.env.METABASE_API_KEY
  });
  
  const success = await client.testConnection();
  
  if (success) {
    console.log(chalk.green('✓ Connection successful!'));
    
    // Show available databases
    const databases = await client.getDatabases();
    console.log(chalk.cyan('\nAvailable databases:'));
    if (Array.isArray(databases)) {
      databases.forEach(db => {
        console.log(chalk.gray(`  - ${db.name} (${db.engine})`));
      });
    } else if (databases && databases.data) {
      databases.data.forEach(db => {
        console.log(chalk.gray(`  - ${db.name} (${db.engine})`));
      });
    } else {
      console.log(chalk.yellow('  No databases found or unexpected format'));
    }
  } else {
    console.log(chalk.red('✗ Connection failed'));
  }
}

function showHelp() {
  console.log(chalk.cyan.bold('\n🤖 Metabase AI Assistant\n'));
  console.log('Usage: npm start [command]');
  console.log('\nCommands:');
  console.log('  (no command)    Start interactive CLI');
  console.log('  test            Test Metabase connection');
  console.log('  help            Show this help message');
  console.log('  version         Show version information');
  console.log('\nEnvironment Variables:');
  console.log('  METABASE_URL       Metabase server URL');
  console.log('  METABASE_USERNAME  Metabase username');
  console.log('  METABASE_PASSWORD  Metabase password');
  console.log('  METABASE_API_KEY   Metabase API key (optional)');
  console.log('  ANTHROPIC_API_KEY  Anthropic API key (for AI features)');
  console.log('  OPENAI_API_KEY     OpenAI API key (alternative to Anthropic)');
  console.log('\nExamples:');
  console.log('  npm start                  # Start interactive mode');
  console.log('  npm start test            # Test connection');
  console.log('  npm run dev               # Start in development mode');
}

async function showVersion() {
  const fs = await import('fs');
  const packageJson = JSON.parse(
    await fs.promises.readFile('./package.json', 'utf8')
  );
  console.log(chalk.cyan(`Metabase AI Assistant v${packageJson.version}`));
}

// Export for programmatic use
export { MetabaseClient, MetabaseAIAssistant };

// Run main function if executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(error => {
    console.error(chalk.red('Fatal error:'), error);
    process.exit(1);
  });
}