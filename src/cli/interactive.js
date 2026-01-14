import inquirer from 'inquirer';
import chalk from 'chalk';
import ora from 'ora';
import { MetabaseClient } from '../metabase/client.js';
import { MetabaseAIAssistant } from '../ai/assistant.js';
import { logger } from '../utils/logger.js';

export class InteractiveCLI {
  constructor(config) {
    this.metabaseClient = new MetabaseClient({
      url: config.metabaseUrl,
      username: config.metabaseUsername,
      password: config.metabasePassword,
      apiKey: config.metabaseApiKey
    });

    this.assistant = new MetabaseAIAssistant({
      metabaseClient: this.metabaseClient,
      aiProvider: config.aiProvider,
      anthropicApiKey: config.anthropicApiKey,
      openaiApiKey: config.openaiApiKey
    });

    this.currentDatabase = null;
    this.currentCollection = null;
  }

  async start() {
    console.log(chalk.cyan.bold('\n🤖 Metabase AI Assistant\n'));
    
    // Test connection
    const spinner = ora('Connecting to Metabase...').start();
    try {
      await this.metabaseClient.testConnection();
      spinner.succeed('Connected to Metabase');
    } catch (error) {
      spinner.fail('Failed to connect to Metabase');
      console.error(chalk.red(error.message));
      return;
    }

    // Select database
    await this.selectDatabase();
    
    // Main menu loop
    await this.mainMenu();
  }

  async selectDatabase() {
    let databases = await this.metabaseClient.getDatabases();

    // Handle different response formats
    if (!Array.isArray(databases)) {
      if (databases && databases.data && Array.isArray(databases.data)) {
        databases = databases.data;
      } else {
        console.log(chalk.yellow('No databases found or unexpected response format'));
        logger.warn('getDatabases returned non-array:', typeof databases);
        return;
      }
    }

    if (databases.length === 0) {
      console.log(chalk.yellow('No databases found in Metabase'));
      return;
    }

    const { database } = await inquirer.prompt([
      {
        type: 'list',
        name: 'database',
        message: 'Select a database:',
        choices: databases.map(db => ({
          name: `${db.name} (${db.engine})`,
          value: db
        }))
      }
    ]);

    this.currentDatabase = database;
    console.log(chalk.green(`✓ Selected database: ${database.name}`));
  }

  async mainMenu() {
    const choices = [
      { name: '📊 Create Model from Description', value: 'create_model' },
      { name: '❓ Create Question (SQL)', value: 'create_question' },
      { name: '📈 Create Metric', value: 'create_metric' },
      { name: '📋 Create Dashboard', value: 'create_dashboard' },
      { name: '🔍 Explore Database Schema', value: 'explore_schema' },
      { name: '🚀 Execute SQL Query', value: 'execute_sql' },
      { name: '🔧 Optimize Existing Query', value: 'optimize_query' },
      { name: '💡 AI Query Builder', value: 'ai_query_builder' },
      { name: '📦 Batch Operations', value: 'batch_operations' },
      { name: '🔄 Switch Database', value: 'switch_database' },
      { name: '❌ Exit', value: 'exit' }
    ];

    const { action } = await inquirer.prompt([
      {
        type: 'list',
        name: 'action',
        message: 'What would you like to do?',
        choices
      }
    ]);

    switch (action) {
      case 'create_model':
        await this.createModel();
        break;
      case 'create_question':
        await this.createQuestion();
        break;
      case 'create_metric':
        await this.createMetric();
        break;
      case 'create_dashboard':
        await this.createDashboard();
        break;
      case 'explore_schema':
        await this.exploreSchema();
        break;
      case 'execute_sql':
        await this.executeSQL();
        break;
      case 'optimize_query':
        await this.optimizeQuery();
        break;
      case 'ai_query_builder':
        await this.aiQueryBuilder();
        break;
      case 'batch_operations':
        await this.batchOperations();
        break;
      case 'switch_database':
        await this.selectDatabase();
        break;
      case 'exit':
        console.log(chalk.cyan('Goodbye! 👋'));
        process.exit(0);
    }

    // Return to main menu
    await this.mainMenu();
  }

  async createModel() {
    const { description } = await inquirer.prompt([
      {
        type: 'input',
        name: 'description',
        message: 'Describe the model you want to create:',
        validate: input => input.length > 0
      }
    ]);

    const spinner = ora('Creating model...').start();
    try {
      const model = await this.assistant.createModel(description, this.currentDatabase.id);
      spinner.succeed(`Model created: ${model.name}`);
      console.log(chalk.gray(`ID: ${model.id}`));
    } catch (error) {
      spinner.fail('Failed to create model');
      console.error(chalk.red(error.message));
    }
  }

  async createQuestion() {
    const { description, useAI } = await inquirer.prompt([
      {
        type: 'confirm',
        name: 'useAI',
        message: 'Use AI to generate SQL?',
        default: true
      },
      {
        type: 'input',
        name: 'description',
        message: 'Describe the question/query:',
        validate: input => input.length > 0
      }
    ]);

    const spinner = ora('Creating question...').start();
    try {
      if (useAI) {
        const question = await this.assistant.createQuestion(
          description,
          this.currentDatabase.id,
          this.currentCollection?.id
        );
        spinner.succeed(`Question created: ${question.name}`);
        console.log(chalk.gray(`ID: ${question.id}`));
      } else {
        const { sql } = await inquirer.prompt([
          {
            type: 'editor',
            name: 'sql',
            message: 'Enter SQL query:'
          }
        ]);
        
        const question = await this.metabaseClient.createSQLQuestion(
          description,
          description,
          this.currentDatabase.id,
          sql,
          this.currentCollection?.id
        );
        spinner.succeed(`Question created: ${question.name}`);
      }
    } catch (error) {
      spinner.fail('Failed to create question');
      console.error(chalk.red(error.message));
    }
  }

  async createMetric() {
    // Get tables
    const tables = await this.metabaseClient.getDatabaseTables(this.currentDatabase.id);
    
    const { table, description } = await inquirer.prompt([
      {
        type: 'list',
        name: 'table',
        message: 'Select table for metric:',
        choices: tables.map(t => ({ name: t.display_name, value: t }))
      },
      {
        type: 'input',
        name: 'description',
        message: 'Describe the metric:',
        validate: input => input.length > 0
      }
    ]);

    const spinner = ora('Creating metric...').start();
    try {
      const metric = await this.assistant.createMetric(description, table.id);
      spinner.succeed(`Metric created: ${metric.name}`);
    } catch (error) {
      spinner.fail('Failed to create metric');
      console.error(chalk.red(error.message));
    }
  }

  async createDashboard() {
    const { name, description, addQuestions } = await inquirer.prompt([
      {
        type: 'input',
        name: 'name',
        message: 'Dashboard name:',
        validate: input => input.length > 0
      },
      {
        type: 'input',
        name: 'description',
        message: 'Dashboard description:'
      },
      {
        type: 'confirm',
        name: 'addQuestions',
        message: 'Add existing questions to dashboard?',
        default: true
      }
    ]);

    let selectedQuestions = [];
    if (addQuestions) {
      const questions = await this.metabaseClient.getQuestions();
      const { selected } = await inquirer.prompt([
        {
          type: 'checkbox',
          name: 'selected',
          message: 'Select questions to add:',
          choices: questions.map(q => ({ name: q.name, value: q }))
        }
      ]);
      selectedQuestions = selected;
    }

    const spinner = ora('Creating dashboard...').start();
    try {
      const dashboard = await this.assistant.createDashboard(description, selectedQuestions);
      spinner.succeed(`Dashboard created: ${name}`);
      console.log(chalk.gray(`URL: ${this.metabaseClient.baseURL}/dashboard/${dashboard.id}`));
    } catch (error) {
      spinner.fail('Failed to create dashboard');
      console.error(chalk.red(error.message));
    }
  }

  async exploreSchema() {
    const schemas = await this.metabaseClient.getDatabaseSchemas(this.currentDatabase.id);
    const tables = await this.metabaseClient.getDatabaseTables(this.currentDatabase.id);
    
    console.log(chalk.cyan('\n📊 Database Schema:\n'));
    console.log(chalk.yellow(`Schemas: ${schemas.join(', ')}`));
    console.log(chalk.yellow(`Tables: ${tables.length}`));
    
    const { viewDetails } = await inquirer.prompt([
      {
        type: 'confirm',
        name: 'viewDetails',
        message: 'View table details?',
        default: false
      }
    ]);

    if (viewDetails) {
      tables.forEach(table => {
        console.log(chalk.green(`\n${table.display_name} (${table.name})`));
        if (table.fields) {
          table.fields.forEach(field => {
            console.log(`  - ${field.display_name}: ${field.base_type}`);
          });
        }
      });
    }
  }

  async executeSQL() {
    const { sql } = await inquirer.prompt([
      {
        type: 'editor',
        name: 'sql',
        message: 'Enter SQL query to execute:'
      }
    ]);

    const spinner = ora('Executing query...').start();
    try {
      const result = await this.metabaseClient.executeNativeQuery(this.currentDatabase.id, sql);
      spinner.succeed('Query executed successfully');
      
      // Display results
      if (result.data && result.data.rows) {
        console.table(result.data.rows.slice(0, 10));
        if (result.data.rows.length > 10) {
          console.log(chalk.gray(`... and ${result.data.rows.length - 10} more rows`));
        }
      }
    } catch (error) {
      spinner.fail('Query execution failed');
      console.error(chalk.red(error.message));
    }
  }

  async optimizeQuery() {
    const { sql } = await inquirer.prompt([
      {
        type: 'editor',
        name: 'sql',
        message: 'Enter SQL query to optimize:'
      }
    ]);

    const spinner = ora('Analyzing query...').start();
    try {
      const optimization = await this.assistant.optimizeQuery(sql);
      spinner.succeed('Query analyzed');
      
      console.log(chalk.cyan('\n🔧 Optimized Query:\n'));
      console.log(chalk.green(optimization.optimized_sql));
      
      console.log(chalk.cyan('\n📋 Optimizations Applied:'));
      optimization.optimizations.forEach(opt => {
        console.log(chalk.yellow(`  • ${opt}`));
      });
      
      console.log(chalk.cyan('\n📈 Expected Improvements:'));
      console.log(chalk.gray(optimization.improvements));
    } catch (error) {
      spinner.fail('Failed to optimize query');
      console.error(chalk.red(error.message));
    }
  }

  async aiQueryBuilder() {
    console.log(chalk.cyan('\n🤖 AI Query Builder\n'));
    console.log(chalk.gray('Describe what data you want in natural language'));
    
    const { description } = await inquirer.prompt([
      {
        type: 'input',
        name: 'description',
        message: 'What data do you need?',
        validate: input => input.length > 0
      }
    ]);

    const spinner = ora('Generating SQL...').start();
    try {
      const tables = await this.metabaseClient.getDatabaseTables(this.currentDatabase.id);
      const sql = await this.assistant.generateSQL(description, tables);
      spinner.succeed('SQL generated');
      
      console.log(chalk.cyan('\n📝 Generated SQL:\n'));
      console.log(chalk.green(sql));
      
      const { action } = await inquirer.prompt([
        {
          type: 'list',
          name: 'action',
          message: 'What would you like to do?',
          choices: [
            { name: 'Execute query', value: 'execute' },
            { name: 'Save as question', value: 'save' },
            { name: 'Explain query', value: 'explain' },
            { name: 'Edit query', value: 'edit' },
            { name: 'Cancel', value: 'cancel' }
          ]
        }
      ]);

      switch (action) {
        case 'execute':
          await this.executeGeneratedSQL(sql);
          break;
        case 'save':
          await this.saveGeneratedSQL(sql, description);
          break;
        case 'explain':
          await this.explainSQL(sql);
          break;
        case 'edit':
          await this.editAndExecuteSQL(sql);
          break;
      }
    } catch (error) {
      spinner.fail('Failed to generate SQL');
      console.error(chalk.red(error.message));
    }
  }

  async batchOperations() {
    const { operation } = await inquirer.prompt([
      {
        type: 'list',
        name: 'operation',
        message: 'Select batch operation:',
        choices: [
          { name: 'Import questions from file', value: 'import_questions' },
          { name: 'Generate multiple metrics', value: 'generate_metrics' },
          { name: 'Create dashboard from template', value: 'template_dashboard' },
          { name: 'Bulk update visualizations', value: 'update_viz' }
        ]
      }
    ]);

    // Implementation for batch operations
    console.log(chalk.yellow('Batch operation: ' + operation));
    console.log(chalk.gray('This feature is under development'));
  }

  // Helper methods
  async executeGeneratedSQL(sql) {
    const spinner = ora('Executing query...').start();
    try {
      const result = await this.metabaseClient.executeNativeQuery(this.currentDatabase.id, sql);
      spinner.succeed('Query executed');
      console.table(result.data.rows.slice(0, 10));
    } catch (error) {
      spinner.fail('Execution failed');
      console.error(chalk.red(error.message));
    }
  }

  async saveGeneratedSQL(sql, description) {
    const question = await this.metabaseClient.createSQLQuestion(
      description,
      description,
      this.currentDatabase.id,
      sql,
      this.currentCollection?.id
    );
    console.log(chalk.green(`✓ Saved as question: ${question.name}`));
  }

  async explainSQL(sql) {
    const spinner = ora('Analyzing query...').start();
    const explanation = await this.assistant.explainQuery(sql);
    spinner.succeed('Analysis complete');
    console.log(chalk.cyan('\n📖 Query Explanation:\n'));
    console.log(chalk.gray(explanation));
  }

  async editAndExecuteSQL(sql) {
    const { editedSQL } = await inquirer.prompt([
      {
        type: 'editor',
        name: 'editedSQL',
        message: 'Edit SQL query:',
        default: sql
      }
    ]);
    await this.executeGeneratedSQL(editedSQL);
  }
}