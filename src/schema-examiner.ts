import * as path from 'path';
import * as glob from 'glob';

// Try to import ParquetJS, fall back if not available
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let parquet: any;
try {
  parquet = require('@dsnp/parquetjs');
} catch (error) {
  parquet = null;
}


// Main execution
async function main() {
  const dataDir = process.argv[2] || '.';
  const searchPattern = path.join(dataDir, 'vessels', '**', '*.parquet');

  console.log(`Searching pattern: ${searchPattern}`);

  const files = glob.sync(searchPattern);
  console.log(`Found ${files.length} parquet files\n`);

  let totalFiles = 0;
  let schemasFound = 0;
  let noSchemasFound = 0;
  let correctSchemas = 0;
  let violationSchemas = 0;
  const vessels = new Set<string>();

  for (const filePath of files) {
  // Skip quarantined files
  if (path.basename(filePath).includes('quarantine') || path.basename(filePath).includes('corrupted')) {
    console.log(`${filePath}: SKIPPED (quarantined)`);
    continue;
  }

  // Extract vessel from path (vessels/[vessel]/)
  const pathParts = filePath.split(path.sep);
  const vesselsIndex = pathParts.findIndex(part => part === 'vessels');
  if (vesselsIndex !== -1 && vesselsIndex + 1 < pathParts.length) {
    vessels.add(pathParts[vesselsIndex + 1]);
  }

  totalFiles++;

  try {
    const reader = await parquet.ParquetReader.openFile(filePath);
    const cursor = reader.getCursor();
    const schema = cursor.schema;

    if (schema && schema.schema) {
      schemasFound++;
      const fields = schema.schema;

      // Check timestamps
      const receivedTimestamp = fields.received_timestamp ? fields.received_timestamp.type : 'MISSING';
      const signalkTimestamp = fields.signalk_timestamp ? fields.signalk_timestamp.type : 'MISSING';

      // Find all value fields
      const valueFields: { [key: string]: string } = {};
      Object.keys(fields).forEach(fieldName => {
        if (fieldName.startsWith('value_') || fieldName === 'value') {
          valueFields[fieldName] = fields[fieldName].type;
        }
      });

      // Check for schema violations
      let hasViolations = false;
      const violations: string[] = [];

      // Rule 1: Timestamps should be UTF8/VARCHAR
      if (receivedTimestamp !== 'UTF8' && receivedTimestamp !== 'MISSING') {
        violations.push(`received_timestamp should be UTF8, got ${receivedTimestamp}`);
        hasViolations = true;
      }
      if (signalkTimestamp !== 'UTF8' && signalkTimestamp !== 'MISSING') {
        violations.push(`signalk_timestamp should be UTF8, got ${signalkTimestamp}`);
        hasViolations = true;
      }

      // Rule 2: Check for suspicious VARCHAR value fields that should likely be numeric
      Object.keys(valueFields).forEach(fieldName => {
        const fieldType = valueFields[fieldName];
        if (fieldType === 'UTF8' || fieldType === 'VARCHAR') {
          // Numeric field names that should probably be DOUBLE
          if (fieldName.includes('latitude') || fieldName.includes('longitude') ||
              fieldName.includes('speed') || fieldName.includes('heading') ||
              fieldName.includes('depth') || fieldName.includes('temperature') ||
              fieldName.includes('pressure') || fieldName.includes('angle') ||
              fieldName.includes('voltage') || fieldName.includes('current')) {
            violations.push(`${fieldName} appears numeric but is ${fieldType}, should likely be DOUBLE`);
            hasViolations = true;
          }
        }
      });

      if (hasViolations) {
        violationSchemas++;
        console.log(`${filePath}: SCHEMA VIOLATIONS`);
        console.log(`  received_timestamp: ${receivedTimestamp}`);
        console.log(`  signalk_timestamp: ${signalkTimestamp}`);
        console.log(`  value fields: ${JSON.stringify(valueFields)}`);
        console.log(`  VIOLATIONS: ${violations.join(', ')}`);
      } else {
        correctSchemas++;
        console.log(`${filePath}: OK`);
        console.log(`  received_timestamp: ${receivedTimestamp}`);
        console.log(`  signalk_timestamp: ${signalkTimestamp}`);
        console.log(`  value fields: ${JSON.stringify(valueFields)}`);
      }

      if (typeof reader.close === 'function') reader.close();
    } else {
      console.log(`${filePath}: No schema found`);
      noSchemasFound++;
      if (typeof reader.close === 'function') reader.close();
    }

  } catch (error) {
    console.log(`${filePath}: ERROR - ${(error as Error).message}`);
    noSchemasFound++;
  }
}

  // Summary report
  console.log(`\n=== SUMMARY ===`);
  console.log(`Total files: ${totalFiles}`);
  console.log(`Total vessels: ${vessels.size}`);
  console.log(`Schemas found: ${schemasFound}`);
  console.log(`No schemas found: ${noSchemasFound}`);
  console.log(`Correct schemas: ${correctSchemas}`);
  console.log(`Schema violations: ${violationSchemas}`);
}

main().catch(console.error);