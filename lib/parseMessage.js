const concepts = require('./concepts');

const DATE_RE = /^(\d{1,2})\/(\d{1,2})(?:\/(\d{2,4}))?$/;

function parseDate(token) {
  const m = token.match(DATE_RE);
  if (!m) return null;
  const day = parseInt(m[1], 10);
  const month = parseInt(m[2], 10);
  let year = m[3] != null ? parseInt(m[3], 10) : new Date().getFullYear();
  if (year < 100) year += 2000;
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;
  return { day, month, year };
}

function resolveConcept(token) {
  if (!token) return concepts[0];
  const n = parseInt(token, 10);
  if (!Number.isNaN(n) && n >= 1 && n <= concepts.length) {
    return concepts[n - 1];
  }
  const code = token.toLowerCase().trim();
  const found = concepts.find((c) => c.code === code);
  return found || null;
}

function parseMessage(text) {
  const t = (text || '').trim();
  if (!t) {
    return { error: 'Escriu almenys la quantitat. Exemple: /o 50 o /o 50 indexa' };
  }

  const parts = t.split(/\s+/);
  let date = null;
  let i = 0;

  if (parts.length > 0) {
    const maybeDate = parseDate(parts[0]);
    if (maybeDate) {
      date = maybeDate;
      i = 1;
    }
  }

  if (!date) {
    const now = new Date();
    date = { day: now.getDate(), month: now.getMonth() + 1, year: now.getFullYear() };
  }

  const rest = parts.slice(i);
  if (rest.length < 1) {
    return { error: 'Indica la quantitat. Exemple: /o 50 o /o 50 2' };
  }

  const amount = parseFloat(rest[0].replace(',', '.'));
  if (Number.isNaN(amount)) {
    return { error: 'La quantitat ha de ser un número.' };
  }

  const conceptToken = rest[1];
  const concept = resolveConcept(conceptToken);
  if (!concept) {
    const codes = concepts.map((c, idx) => `${idx + 1}=${c.code}`).join(', ');
    return { error: `Concepte no vàlid. Opcions: ${codes}` };
  }

  const dateStr =
    String(date.day).padStart(2, '0') +
    '/' +
    String(date.month).padStart(2, '0') +
    '/' +
    date.year;

  return {
    dateStr,
    month: date.month,
    year: date.year,
    type: 'Cash',
    category: concept.category,
    entity: concept.entity,
    amount,
    conceptLabel: concept.label,
  };
}

function rowToSheetRow(row) {
  return [row.dateStr, row.month, row.year, row.type, row.category, row.entity, row.amount];
}

function parseBulkRow(parts) {
  if (parts.length !== 7) return { error: `Falten dades: calen 7 valors (data, mes, any, tipo, categoria, entitat, quantitat). N'has posat ${parts.length}.` };
  const [dateStr, monthStr, yearStr, type, category, entity, amountStr] = parts;
  const month = parseInt(monthStr, 10);
  const year = parseInt(yearStr, 10);
  const amount = parseFloat(String(amountStr).replace(',', '.'));
  if (Number.isNaN(month) || month < 1 || month > 12) return { error: `El mes no és vàlid: ${monthStr}` };
  if (Number.isNaN(year) || year < 1900 || year > 2100) return { error: `L'any no és vàlid: ${yearStr}` };
  if (Number.isNaN(amount)) return { error: `La quantitat no és vàlida: ${amountStr}` };
  return { dateStr, month, year, type, category, entity, amount };
}

function parseBulkLines(text) {
  const lines = (text || '').split(/\n/).map((l) => l.trim()).filter(Boolean);
  const tokens = [];
  for (const line of lines) {
    const parts = line.split(/[\t,]+/).map((p) => p.trim()).filter(Boolean);
    tokens.push(...parts);
  }
  const rows = [];
  const errors = [];
  for (let i = 0; i + 7 <= tokens.length; i += 7) {
    const chunk = tokens.slice(i, i + 7);
    const result = parseBulkRow(chunk);
    if (result.error) {
      errors.push(`Fila ${Math.floor(i / 7) + 1}: ${result.error}`);
      continue;
    }
    rows.push(result);
  }
  if (tokens.length % 7 !== 0 && rows.length === 0 && errors.length === 0) {
    errors.push(`Falten valors: has posat ${tokens.length} valors. Han de ser múltiple de 7 (7 per fila).`);
  }
  return { rows, errors };
}

module.exports = { parseMessage, rowToSheetRow, parseBulkLines };