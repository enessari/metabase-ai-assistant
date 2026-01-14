import fs from 'fs/promises';
import path from 'path';
import { logger } from './logger.js';

export class FileOperations {
  constructor(options = {}) {
    this.baseDir = options.baseDir || './exports';
    this.allowedExtensions = options.allowedExtensions || ['.json', '.csv', '.sql', '.md', '.txt'];
    this.maxFileSize = options.maxFileSize || 10 * 1024 * 1024; // 10MB
    
    // Ensure base directory exists
    this.ensureBaseDirectory();
  }

  async ensureBaseDirectory() {
    try {
      await fs.mkdir(this.baseDir, { recursive: true });
    } catch (error) {
      logger.error('Failed to create base directory:', error);
    }
  }

  validatePath(filePath) {
    // Security checks
    if (filePath.includes('..')) {
      throw new Error('Path traversal not allowed');
    }
    
    const ext = path.extname(filePath).toLowerCase();
    if (!this.allowedExtensions.includes(ext)) {
      throw new Error(`File extension not allowed: ${ext}`);
    }
    
    return true;
  }

  async writeFile(filename, content, options = {}) {
    try {
      this.validatePath(filename);
      
      const filePath = path.join(this.baseDir, filename);
      const dir = path.dirname(filePath);
      
      // Ensure directory exists
      await fs.mkdir(dir, { recursive: true });
      
      // Size check
      const contentSize = Buffer.byteLength(content, 'utf8');
      if (contentSize > this.maxFileSize) {
        throw new Error(`File size exceeds limit: ${contentSize} > ${this.maxFileSize}`);
      }
      
      await fs.writeFile(filePath, content, options.encoding || 'utf8');
      
      logger.info(`File written successfully: ${filePath}`);
      return {
        success: true,
        path: filePath,
        size: contentSize
      };
      
    } catch (error) {
      logger.error(`Failed to write file ${filename}:`, error);
      throw error;
    }
  }

  async readFile(filename, options = {}) {
    try {
      this.validatePath(filename);
      
      const filePath = path.join(this.baseDir, filename);
      const content = await fs.readFile(filePath, options.encoding || 'utf8');
      
      const stats = await fs.stat(filePath);
      
      return {
        success: true,
        content,
        path: filePath,
        size: stats.size,
        modified: stats.mtime
      };
      
    } catch (error) {
      logger.error(`Failed to read file ${filename}:`, error);
      throw error;
    }
  }

  async deleteFile(filename) {
    try {
      this.validatePath(filename);
      
      const filePath = path.join(this.baseDir, filename);
      await fs.unlink(filePath);
      
      logger.info(`File deleted successfully: ${filePath}`);
      return { success: true, path: filePath };
      
    } catch (error) {
      logger.error(`Failed to delete file ${filename}:`, error);
      throw error;
    }
  }

  async listFiles(directory = '') {
    try {
      const listDir = path.join(this.baseDir, directory);
      const files = await fs.readdir(listDir, { withFileTypes: true });
      
      const result = [];
      
      for (const file of files) {
        const filePath = path.join(directory, file.name);
        
        if (file.isDirectory()) {
          result.push({
            name: file.name,
            type: 'directory',
            path: filePath
          });
        } else {
          const stats = await fs.stat(path.join(listDir, file.name));
          result.push({
            name: file.name,
            type: 'file',
            path: filePath,
            size: stats.size,
            modified: stats.mtime,
            extension: path.extname(file.name).toLowerCase()
          });
        }
      }
      
      return result;
      
    } catch (error) {
      logger.error(`Failed to list files in ${directory}:`, error);
      throw error;
    }
  }

  async createDirectory(dirPath) {
    try {
      const fullPath = path.join(this.baseDir, dirPath);
      await fs.mkdir(fullPath, { recursive: true });
      
      logger.info(`Directory created successfully: ${fullPath}`);
      return { success: true, path: fullPath };
      
    } catch (error) {
      logger.error(`Failed to create directory ${dirPath}:`, error);
      throw error;
    }
  }

  // Metabase-specific export functions
  async exportDashboard(dashboard, format = 'json') {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `dashboard-${dashboard.id}-${timestamp}.${format}`;
    
    let content;
    
    switch (format) {
      case 'json':
        content = JSON.stringify(dashboard, null, 2);
        break;
      case 'md':
        content = this.dashboardToMarkdown(dashboard);
        break;
      default:
        throw new Error(`Unsupported format: ${format}`);
    }
    
    return await this.writeFile(`dashboards/${filename}`, content);
  }

  async exportQuestion(question, format = 'json') {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `question-${question.id}-${timestamp}.${format}`;
    
    let content;
    
    switch (format) {
      case 'json':
        content = JSON.stringify(question, null, 2);
        break;
      case 'sql':
        content = question.dataset_query?.native?.query || 'No SQL query found';
        break;
      case 'md':
        content = this.questionToMarkdown(question);
        break;
      default:
        throw new Error(`Unsupported format: ${format}`);
    }
    
    return await this.writeFile(`questions/${filename}`, content);
  }

  async exportQueryResults(results, filename, format = 'csv') {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const finalFilename = `${filename}-${timestamp}.${format}`;
    
    let content;
    
    switch (format) {
      case 'csv':
        content = this.resultsToCSV(results);
        break;
      case 'json':
        content = JSON.stringify(results, null, 2);
        break;
      default:
        throw new Error(`Unsupported format: ${format}`);
    }
    
    return await this.writeFile(`results/${finalFilename}`, content);
  }

  async exportModel(model, format = 'json') {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `model-${model.id}-${timestamp}.${format}`;
    
    let content;
    
    switch (format) {
      case 'json':
        content = JSON.stringify(model, null, 2);
        break;
      case 'sql':
        content = model.dataset_query?.native?.query || 'No SQL query found';
        break;
      case 'md':
        content = this.modelToMarkdown(model);
        break;
      default:
        throw new Error(`Unsupported format: ${format}`);
    }
    
    return await this.writeFile(`models/${filename}`, content);
  }

  // Import functions
  async importDashboard(filename) {
    const file = await this.readFile(`dashboards/${filename}`);
    const dashboard = JSON.parse(file.content);
    
    // Validate dashboard structure
    if (!dashboard.name || !dashboard.ordered_cards) {
      throw new Error('Invalid dashboard structure');
    }
    
    return dashboard;
  }

  async importQuestion(filename) {
    const file = await this.readFile(`questions/${filename}`);
    const question = JSON.parse(file.content);
    
    // Validate question structure
    if (!question.name || !question.dataset_query) {
      throw new Error('Invalid question structure');
    }
    
    return question;
  }

  // Backup and restore functions
  async backupMetabaseConfig(config) {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `backup-${timestamp}.json`;
    
    const backupData = {
      timestamp: new Date().toISOString(),
      version: '1.0',
      config,
      metadata: {
        exported_by: 'metabase-ai-assistant',
        export_type: 'full_backup'
      }
    };
    
    return await this.writeFile(`backups/${filename}`, JSON.stringify(backupData, null, 2));
  }

  async restoreMetabaseConfig(filename) {
    const file = await this.readFile(`backups/${filename}`);
    const backup = JSON.parse(file.content);
    
    if (!backup.config || !backup.version) {
      throw new Error('Invalid backup file structure');
    }
    
    return backup;
  }

  // Format conversion helpers
  dashboardToMarkdown(dashboard) {
    let md = `# ${dashboard.name}\n\n`;
    
    if (dashboard.description) {
      md += `${dashboard.description}\n\n`;
    }
    
    md += `**Dashboard ID:** ${dashboard.id}\n`;
    md += `**Created:** ${dashboard.created_at}\n`;
    md += `**Updated:** ${dashboard.updated_at}\n\n`;
    
    if (dashboard.ordered_cards && dashboard.ordered_cards.length > 0) {
      md += `## Cards (${dashboard.ordered_cards.length})\n\n`;
      
      dashboard.ordered_cards.forEach((card, index) => {
        md += `### ${index + 1}. ${card.card?.name || 'Unnamed Card'}\n`;
        if (card.card?.description) {
          md += `${card.card.description}\n`;
        }
        md += `- Position: Row ${card.row}, Col ${card.col}\n`;
        md += `- Size: ${card.sizeX} x ${card.sizeY}\n\n`;
      });
    }
    
    return md;
  }

  questionToMarkdown(question) {
    let md = `# ${question.name}\n\n`;
    
    if (question.description) {
      md += `${question.description}\n\n`;
    }
    
    md += `**Question ID:** ${question.id}\n`;
    md += `**Database:** ${question.database_id}\n`;
    md += `**Visualization:** ${question.display || 'table'}\n`;
    md += `**Created:** ${question.created_at}\n`;
    md += `**Updated:** ${question.updated_at}\n\n`;
    
    if (question.dataset_query?.native?.query) {
      md += `## SQL Query\n\n\`\`\`sql\n${question.dataset_query.native.query}\n\`\`\`\n\n`;
    }
    
    return md;
  }

  modelToMarkdown(model) {
    let md = `# ${model.name}\n\n`;
    
    if (model.description) {
      md += `${model.description}\n\n`;
    }
    
    md += `**Model ID:** ${model.id}\n`;
    md += `**Database:** ${model.database_id}\n`;
    md += `**Created:** ${model.created_at}\n`;
    md += `**Updated:** ${model.updated_at}\n\n`;
    
    if (model.dataset_query?.native?.query) {
      md += `## SQL Query\n\n\`\`\`sql\n${model.dataset_query.native.query}\n\`\`\`\n\n`;
    }
    
    return md;
  }

  resultsToCSV(results) {
    if (!results.data || !results.data.cols || !results.data.rows) {
      throw new Error('Invalid results structure for CSV export');
    }
    
    const headers = results.data.cols.map(col => col.display_name || col.name);
    const rows = results.data.rows;
    
    let csv = headers.map(h => `"${h}"`).join(',') + '\n';
    
    for (const row of rows) {
      const escapedRow = row.map(cell => {
        if (cell === null || cell === undefined) return '';
        const str = String(cell);
        if (str.includes(',') || str.includes('"') || str.includes('\n')) {
          return `"${str.replace(/"/g, '""')}"`;
        }
        return str;
      });
      csv += escapedRow.join(',') + '\n';
    }
    
    return csv;
  }

  // Batch operations
  async batchExport(items, type, format = 'json') {
    const results = [];
    
    for (const item of items) {
      try {
        let result;
        
        switch (type) {
          case 'dashboard':
            result = await this.exportDashboard(item, format);
            break;
          case 'question':
            result = await this.exportQuestion(item, format);
            break;
          case 'model':
            result = await this.exportModel(item, format);
            break;
          default:
            throw new Error(`Unsupported export type: ${type}`);
        }
        
        results.push({
          item_id: item.id,
          success: true,
          path: result.path
        });
        
      } catch (error) {
        results.push({
          item_id: item.id,
          success: false,
          error: error.message
        });
        logger.error(`Failed to export ${type} ${item.id}:`, error);
      }
    }
    
    return results;
  }

  // Cleanup utilities
  async cleanupOldFiles(maxAge = 7 * 24 * 60 * 60 * 1000) { // 7 days
    try {
      const files = await this.listFiles();
      const now = new Date().getTime();
      const deleted = [];
      
      for (const file of files) {
        if (file.type === 'file' && file.modified) {
          const fileAge = now - new Date(file.modified).getTime();
          if (fileAge > maxAge) {
            await this.deleteFile(file.path);
            deleted.push(file.path);
          }
        }
      }
      
      logger.info(`Cleaned up ${deleted.length} old files`);
      return deleted;
      
    } catch (error) {
      logger.error('Failed to cleanup old files:', error);
      throw error;
    }
  }

  async getStorageStats() {
    try {
      const files = await this.listFiles();
      let totalSize = 0;
      let fileCount = 0;
      let dirCount = 0;
      
      const typeStats = {};
      
      for (const file of files) {
        if (file.type === 'file') {
          totalSize += file.size || 0;
          fileCount++;
          
          const ext = file.extension;
          if (!typeStats[ext]) {
            typeStats[ext] = { count: 0, size: 0 };
          }
          typeStats[ext].count++;
          typeStats[ext].size += file.size || 0;
        } else {
          dirCount++;
        }
      }
      
      return {
        totalSize,
        fileCount,
        dirCount,
        typeStats,
        basePath: this.baseDir
      };
      
    } catch (error) {
      logger.error('Failed to get storage stats:', error);
      throw error;
    }
  }
}