const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');

let sheets = null;
const DEFAULT_RANGE = 'A:G';

function getCredentials() {
  const credPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  if (credPath) {
    const fullPath = path.isAbsolute(credPath) ? credPath : path.resolve(process.cwd(), credPath);
    if (!fs.existsSync(fullPath)) {
      throw new Error('Credentials file not found: ' + fullPath);
    }
    return JSON.parse(fs.readFileSync(fullPath, 'utf8'));
  }

  const raw = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!raw) {
    throw new Error(
      'Add to .env either GOOGLE_APPLICATION_CREDENTIALS (path to JSON) or GOOGLE_SERVICE_ACCOUNT_JSON (content)'
    );
  }

  try {
    const json = raw.startsWith('{') ? raw : Buffer.from(raw, 'base64').toString('utf8');
    return JSON.parse(json);
  } catch (e) {
    throw new Error('GOOGLE_SERVICE_ACCOUNT_JSON value is not valid JSON');
  }
}

function getSheetsClient() {
  if (sheets) return sheets;

  const credentials = getCredentials();

  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  sheets = google.sheets({ version: 'v4', auth });
  return sheets;
}

async function getSheetTabs(spreadsheetId) {
  const client = getSheetsClient();
  const res = await client.spreadsheets.get({
    spreadsheetId,
    fields: 'sheets.properties(sheetId,title)',
  });
  return (res.data.sheets || []).map((s) => ({
    sheetId: s.properties.sheetId,
    title: s.properties.title || '',
  }));
}

async function getTargetSheetName(spreadsheetId) {
  const tabs = await getSheetTabs(spreadsheetId);
  if (tabs.length === 0) throw new Error('Spreadsheet has no sheets.');
  return tabs[0].title;
}

function getRange() {
  if (process.env.SHEET_RANGE) {
    const r = process.env.SHEET_RANGE;
    const sheetPart = (r.split('!')[0] || '').replace(/'/g, '');
    return { full: r, sheetName: sheetPart };
  }
  return null;
}

async function getSheetRange(spreadsheetId) {
  const sheetName = await getTargetSheetName(spreadsheetId);
  const full = `'${sheetName.replace(/'/g, "''")}'!${DEFAULT_RANGE}`;
  return { full, sheetName };
}

async function appendRow(spreadsheetId, row) {
  const client = getSheetsClient();
  const rangeInfo = getRange() || (await getSheetRange(spreadsheetId));
  await client.spreadsheets.values.append({
    spreadsheetId,
    range: rangeInfo.full,
    valueInputOption: 'USER_ENTERED',
    requestBody: {
      values: [row],
    },
  });
}

async function getLastDataRow(spreadsheetId) {
  const client = getSheetsClient();
  const rangeInfo = getRange() || (await getSheetRange(spreadsheetId));
  const readRange = `${rangeInfo.full}`.split('!')[0] + '!A:A';
  const res = await client.spreadsheets.values.get({
    spreadsheetId,
    range: readRange,
  });
  const rows = res.data.values;
  if (!rows || rows.length === 0) return 0;
  return rows.length;
}
async function clearLastRow(spreadsheetId) {
  const client = getSheetsClient();
  const rangeInfo = getRange() || (await getSheetRange(spreadsheetId));
  const lastRow = await getLastDataRow(spreadsheetId);
  if (lastRow < 1) return;
  const sheetName = rangeInfo.sheetName;
  const range = `'${sheetName.replace(/'/g, "''")}'!A${lastRow}:G${lastRow}`;
  await client.spreadsheets.values.update({
    spreadsheetId,
    range,
    valueInputOption: 'USER_ENTERED',
    requestBody: {
      values: [['', '', '', '', '', '', '']],
    },
  });
}

async function getValues(spreadsheetId) {
  const client = getSheetsClient();
  const rangeInfo = getRange() || (await getSheetRange(spreadsheetId));
  const res = await client.spreadsheets.values.get({
    spreadsheetId,
    range: rangeInfo.full,
  });
  return res.data.values || [];
}

async function getRowIndicesForMonth(spreadsheetId, month, year) {
  const rows = await getValues(spreadsheetId);
  const indices = [];
  const monthNum = Number(month);
  const yearNum = Number(year);
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    if (row.length >= 3) {
      const rowMonth = Number(row[1]);
      const rowYear = Number(row[2]);
      if (!Number.isNaN(rowMonth) && !Number.isNaN(rowYear) && rowMonth === monthNum && rowYear === yearNum) {
        indices.push(i + 1);
      }
    }
  }
  return indices;
}

async function getRowsDataForMonth(spreadsheetId, month, year) {
  const rows = await getValues(spreadsheetId);
  const result = [];
  const monthNum = Number(month);
  const yearNum = Number(year);
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    if (row.length >= 7) {
      const rowMonth = Number(row[1]);
      const rowYear = Number(row[2]);
      if (!Number.isNaN(rowMonth) && !Number.isNaN(rowYear) && rowMonth === monthNum && rowYear === yearNum) {
        result.push(row.slice(0, 7));
      }
    }
  }
  return result;
}

async function copyMonthToMonth(spreadsheetId, fromMonth, fromYear, toMonth, toYear) {
  const rows = await getRowsDataForMonth(spreadsheetId, fromMonth, fromYear);
  const dateStrTo = `01/${String(toMonth).padStart(2, '0')}/${toYear}`;
  for (const row of rows) {
    const newRow = [dateStrTo, toMonth, toYear, row[3], row[4], row[5], row[6]];
    await appendRow(spreadsheetId, newRow);
  }
}

async function getSheetId(spreadsheetId) {
  const rangeInfo = getRange() || (await getSheetRange(spreadsheetId));
  const sheetName = rangeInfo.sheetName;
  const tabs = await getSheetTabs(spreadsheetId);
  const tab = tabs.find((t) => t.title === sheetName);
  if (tab) return tab.sheetId;
  if (tabs.length === 0) throw new Error('Spreadsheet has no sheets.');
  return tabs[0].sheetId;
}

async function deleteRows(spreadsheetId, rowIndices) {
  if (rowIndices.length === 0) return;
  const sheetId = await getSheetId(spreadsheetId);
  const client = getSheetsClient();
  const sorted = [...rowIndices].sort((a, b) => b - a);
  const requests = sorted.map((rowIndex) => ({
    deleteDimension: {
      range: {
        sheetId,
        dimension: 'ROWS',
        startIndex: rowIndex - 1,
        endIndex: rowIndex,
      },
    },
  }));
  await client.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: { requests },
  });
}

module.exports = {
  appendRow,
  getSheetsClient,
  getSheetTabs,
  getTargetSheetName,
  clearLastRow,
  getRowIndicesForMonth,
  getRowsDataForMonth,
  copyMonthToMonth,
  deleteRows,
};
