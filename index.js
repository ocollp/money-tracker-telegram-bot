require('dotenv').config();

const { createBot } = require('./lib/telegram');
const { parseMessage, rowToSheetRow, parseBulkLines } = require('./lib/parseMessage');
const { appendRow, getSheetsClient, clearLastRow, getRowIndicesForMonth, deleteRows, copyMonthToMonth } = require('./lib/sheets');

const spreadsheetId1 = process.env.SPREADSHEET_ID;
const spreadsheetId2 = process.env.SPREADSHEET_ID_2;
const GOOGLE_FORM_URL_01 = process.env.GOOGLE_FORM_URL_01 || '';
const GOOGLE_FORM_URL_02 = process.env.GOOGLE_FORM_URL_02 || process.env.GOOGLE_FORM_URL || '';

function getWelcomeMessage() {
  const olgaUrl = GOOGLE_FORM_URL_01 && GOOGLE_FORM_URL_01.startsWith('http') ? GOOGLE_FORM_URL_01 : '(set GOOGLE_FORM_URL_01 in .env)';
  const andreaUrl = GOOGLE_FORM_URL_02 && GOOGLE_FORM_URL_02.startsWith('http') ? GOOGLE_FORM_URL_02 : '(set GOOGLE_FORM_URL_02 in .env)';
  return `👩🏼 Olga: ${olgaUrl}\n👩🏻 Andrea: ${andreaUrl}`;
}

function getResetMessage() {
  return 'Reset. ' + getWelcomeMessage();
}

const USER_1 = 1;
const USER_2 = 2;

const userByChat = new Map();
const stateByChat = new Map();
const MAX_PROCESSED_UPDATES = 5000;
const processedUpdateIds = new Set();
function wasProcessed(updateId) {
  if (processedUpdateIds.has(updateId)) return true;
  if (processedUpdateIds.size >= MAX_PROCESSED_UPDATES) {
    const oldest = processedUpdateIds.values().next().value;
    processedUpdateIds.delete(oldest);
  }
  processedUpdateIds.add(updateId);
  return false;
}

function nameToUserId(name) {
  if (name === 'olga') return USER_1;
  if (name === 'andrea') return USER_2;
  return null;
}

function getDisplayName(userId) {
  if (userId === USER_1) return 'Olga';
  if (userId === USER_2) return 'Andrea';
  return '';
}

function firstDayOfMonthRow() {
  const now = new Date();
  let month = now.getMonth() + 1;
  let year = now.getFullYear();
  if (now.getDate() > 1) {
    month += 1;
    if (month > 12) {
      month = 1;
      year += 1;
    }
  }
  const dateStr = `01/${String(month).padStart(2, '0')}/${year}`;
  return { dateStr, month, year };
}

function nextMonthFrom(base) {
  let month = base.month + 1;
  let year = base.year;
  if (month > 12) {
    month = 1;
    year += 1;
  }
  return {
    dateStr: `01/${String(month).padStart(2, '0')}/${year}`,
    month,
    year,
  };
}

function previousMonth(base) {
  let month = base.month - 1;
  let year = base.year;
  if (month < 1) {
    month = 12;
    year -= 1;
  }
  return {
    dateStr: `01/${String(month).padStart(2, '0')}/${year}`,
    month,
    year,
  };
}

const CONCEPTS = [
  { id: 1, label: 'Cuenta ahorro · CaixaBank', category: 'Cuenta ahorro', entity: 'CaixaBank', type: 'Cash' },
  { id: 2, label: 'Cuenta corriente · CaixaBank', category: 'Cuenta corriente', entity: 'CaixaBank', type: 'Cash' },
  { id: 3, label: 'Cash · Revolut', category: 'Cash', entity: 'Revolut', type: 'Cash' },
  { id: 4, label: 'Cuenta compartida flexible · Revolut', category: 'Cuenta compartida flexible', entity: 'Revolut', type: 'Cash' },
  { id: 5, label: 'Acciones · Revolut', category: 'Acciones', entity: 'Revolut', type: 'Invertido' },
  { id: 6, label: 'Cryptos · Revolut', category: 'Cryptos', entity: 'Revolut', type: 'Invertido' },
  { id: 7, label: 'Cash · Efectivo', category: 'Cash', entity: 'Efectivo', type: 'Cash' },
  { id: 8, label: 'Cuenta flexible · Trade Republic', category: 'Cuenta flexible', entity: 'Trade Republic', type: 'Cash' },
  { id: 9, label: 'Crowfunding · Fundeen', category: 'Crowfunding', entity: 'Fundeen', type: 'Invertido' },
  { id: 10, label: 'Crowfunding · Urbanitae', category: 'Crowfunding', entity: 'Urbanitae', type: 'Invertido' },
  { id: 11, label: 'Fondo indexado · Indexa Capital', category: 'Fondo indexado', entity: 'Indexa Capital', type: 'Invertido' },
  { id: 12, label: 'Plan de pensiones · Indexa Capital', category: 'Plan de pensiones', entity: 'Indexa Capital', type: 'Invertido' },
  { id: 13, label: 'Vivienda personal · BBVA', category: 'Vivienda personal', entity: 'BBVA', type: 'Invertido', fixedAmount: 150000 },
  { id: 14, label: 'Hipoteca · BBVA', category: 'Hipoteca', entity: 'BBVA', type: 'Invertido' },
];

const ANDREA_CONCEPTS = [
  { id: 1, label: 'Cuenta corriente · BBVA', category: 'Cuenta corriente', entity: 'BBVA', type: 'Cash' },
  { id: 2, label: 'Cuenta corriente · Revolut', category: 'Cuenta corriente', entity: 'Revolut', type: 'Cash' },
  { id: 3, label: 'Cuenta flexible · Revolut', category: 'Cuenta flexible', entity: 'Revolut', type: 'Cash' },
  { id: 4, label: 'Cuenta compartida flexible · Revolut', category: 'Cuenta compartida flexible', entity: 'Revolut', type: 'Cash' },
  { id: 5, label: 'Cuenta flexible · Trade Republic', category: 'Cuenta flexible', entity: 'Trade Republic', type: 'Cash' },
  { id: 6, label: 'Efectivo · Efectivo', category: 'Cash', entity: 'Efectivo', type: 'Cash' },
  { id: 7, label: 'Fondo indexado · Indexa Capital', category: 'Fondo indexado', entity: 'Indexa Capital', type: 'Invertido' },
  { id: 8, label: 'Plan de pensiones · Indexa Capital', category: 'Plan de pensiones', entity: 'Indexa Capital', type: 'Invertido' },
  { id: 9, label: 'Vivienda personal · BBVA', category: 'Vivienda personal', entity: 'BBVA', type: 'Invertido', fixedAmount: 150000 },
  { id: 10, label: 'Hipoteca · BBVA', category: 'Hipoteca', entity: 'BBVA', type: 'Invertido' },
];

function getConceptsForUser(user) {
  return user === USER_2 ? ANDREA_CONCEPTS : CONCEPTS;
}

function getConceptById(id) {
  const n = parseInt(id, 10);
  if (Number.isNaN(n) || n < 1 || n > CONCEPTS.length) return null;
  return CONCEPTS[n - 1];
}

function buildRow(concept, amount, baseOverride) {
  const base = baseOverride || firstDayOfMonthRow();
  return { ...base, type: concept.type, category: concept.category, entity: concept.entity, amount };
}

function conceptsListMessage() {
  return CONCEPTS.map((c) => `${c.id}. ${c.label}`).join('\n');
}

const OLGA_ENTITIES_MESSAGE = `Send one message with all values (one line per concept, *N. amount* in €). Example: 1. 500, 2. 400…

Compte d'estalvis (La Caixa): 
Compte corrent (La Caixa): 
Compte corrent (Revolut): 
Compte compartit flexible (Revolut): 
Accions (Revolut): 
Cryptos (Revolut): 
Efectiu: 
Trade Republic: 
Fundeen: 
Urbanitae: 
Fons indexat (Indexa Capital): 
Pla de pensions (Indexa Capital): 
Hipoteca (el que queda per pagar): `;

function normalizeLabel(s) {
  return (s || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/\p{M}/gu, '')
    .replace(/\s+/g, ' ')
    .trim();
}

const OLGA_LABEL_TO_INDEX = {};
[
  ["Compte d'estalvis (La Caixa)", 0],
  ['Compte corrent (La Caixa)', 1],
  ['Compte corrent (Revolut)', 2],
  ['Compte compartit flexible (Revolut)', 3],
  ['Accions (Revolut)', 4],
  ['Cryptos (Revolut)', 5],
  ['Efectiu', 6],
  ['Trade Republic', 7],
  ['Fundeen', 8],
  ['Urbanitae', 9],
  ['Fons indexat (Indexa Capital)', 10],
  ['Pla de pensions (Indexa Capital)', 11],
  ['Vivienda personal', 12],
  ['Hipoteca', 13],
  ['Hipoteca (el que queda per pagar)', 13],
].forEach(([label, idx]) => {
  OLGA_LABEL_TO_INDEX[normalizeLabel(label)] = idx;
});

const ANDREA_ENTITIES_MESSAGE = `If you're Andrea, send the values (you can copy and paste with format *Label: amount*):

Compte corrent (BBVA): 
Compte corrent (Revolut): 
Compte flexible (Revolut): 
Compte compartit flexible (Revolut): 
Compte flexible (Trade Republic): 
Efectiu: 
Fons indexat (Indexa Capital): 
Pla de pensions (Indexa Capital): 
Hipoteca (el que queda per pagar): `;

const ANDREA_LABEL_TO_INDEX = {};
[
  ['Compte corrent (BBVA)', 0],
  ['Cuenta corriente (BBVA)', 0],
  ['Compte corrent (Revolut)', 1],
  ['Cuenta corriente (Revolut)', 1],
  ['Compte flexible (Revolut)', 2],
  ['Cuenta flexible (Revolut)', 2],
  ['Compte compartit flexible (Revolut)', 3],
  ['Cuenta compartida flexible (Revolut)', 3],
  ['Compte flexible (Trade Republic)', 4],
  ['Cuenta flexible (Trade Republic)', 4],
  ['Efectiu', 5],
  ['Efectivo', 5],
  ['Fons indexat (Indexa Capital)', 6],
  ['Fondo indexado (Indexa Capital)', 6],
  ['Pla de pensions (Indexa Capital)', 7],
  ['Plan de pensiones (Indexa Capital)', 7],
  ['Vivienda personal', 8],
  ['Hipoteca', 9],
  ['Hipoteca (el que queda per pagar)', 9],
  ['Hipoteca (el que queda per pagar', 9],
].forEach(([label, idx]) => {
  ANDREA_LABEL_TO_INDEX[normalizeLabel(label)] = idx;
});

const OLGA_SUMMARY_LABELS = [
  "Compte d'estalvis (La Caixa)",
  'Compte corrent (La Caixa)',
  'Compte corrent (Revolut)',
  'Compte compartit flexible (Revolut)',
  'Accions (Revolut)',
  'Cryptos (Revolut)',
  'Efectiu',
  'Trade Republic',
  'Fundeen',
  'Urbanitae',
  'Fons indexat (Indexa Capital)',
  'Pla de pensions (Indexa Capital)',
  'Vivienda personal',
  'Hipoteca',
];

const ANDREA_SUMMARY_LABELS = [
  'Compte corrent (BBVA)',
  'Compte corrent (Revolut)',
  'Compte flexible (Revolut)',
  'Compte compartit flexible (Revolut)',
  'Compte flexible (Trade Republic)',
  'Efectiu',
  'Fons indexat (Indexa Capital)',
  'Pla de pensions (Indexa Capital)',
  'Vivienda personal',
  'Hipoteca',
];

function getSummaryLabel(user, conceptIndex) {
  const labels = user === USER_2 ? ANDREA_SUMMARY_LABELS : OLGA_SUMMARY_LABELS;
  return labels[conceptIndex] || 'Hipoteca';
}

function parseLabelMessage(text, labelToIndex) {
  const entries = [];
  const errors = [];
  const lines = (text || '').split(/\r?\n|\r/).map((l) => l.trim()).filter(Boolean);
  for (let i = 0; i < lines.length; i++) {
    let line = lines[i];
    const lastColon = line.lastIndexOf(':');
    if (lastColon === -1) continue;
    const labelPart = line.substring(0, lastColon).trim();
    const amountStr = line
      .substring(lastColon + 1)
      .trim()
      .replace(/\s/g, '')
      .replace(',', '.');
    const amount = parseFloat(amountStr);
    if (Number.isNaN(amount)) continue;
    const key = normalizeLabel(labelPart);
    const conceptIndex = labelToIndex[key];
    if (conceptIndex === undefined) {
      errors.push(`Línia ${i + 1}: no reconec "${labelPart}"`);
      continue;
    }
    entries.push({ conceptIndex, amount });
  }
  return { entries, errors };
}

function parseOlgaLabelMessage(text) {
  return parseLabelMessage(text, OLGA_LABEL_TO_INDEX);
}

function parseAndreaLabelMessage(text) {
  return parseLabelMessage(text, ANDREA_LABEL_TO_INDEX);
}

function parseBulkMessage(text, conceptsArray = CONCEPTS) {
  const maxNum = conceptsArray.length;
  const entries = [];
  const errors = [];
  const lines = (text || '').split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  const lineRe = /^\s*(\d{1,2})\s*[.:]\s*([\d\s.,]*)\s*$/;

  for (let i = 0; i < lines.length; i++) {
    let line = lines[i];
    let m = line.match(lineRe);
    if (!m && /^\s*\d{1,2}\s*[.:]\s+.+\s+[\d\s.,]+$/.test(line)) {
      const lastNum = line.match(/\s+([\d\s.,]+)\s*$/);
      if (lastNum) {
        const num = parseInt(line.match(/^\s*(\d{1,2})/)[1], 10);
        const amountStr = (lastNum[1] || '').replace(/\s/g, '').replace(',', '.');
        const amount = parseFloat(amountStr);
        if (num >= 1 && num <= maxNum && !Number.isNaN(amount) && amount >= 0) {
          const concept = conceptsArray[num - 1];
          const amt = concept?.fixedAmount != null && amount === 0 ? concept.fixedAmount : amount;
          entries.push({ conceptIndex: num - 1, amount: amt });
        }
      }
      continue;
    }
    if (!m) continue;
    const num = parseInt(m[1], 10);
    const amountStr = (m[2] || '').replace(/\s/g, '').replace(',', '.');
    let amount = amountStr === '' ? NaN : parseFloat(amountStr);
    if (num < 1 || num > maxNum) {
      errors.push(`Línia ${i + 1}: número 1-${maxNum}`);
      continue;
    }
    const concept = conceptsArray[num - 1];
    if (concept?.fixedAmount != null && (Number.isNaN(amount) || amount === 0)) {
      amount = concept.fixedAmount;
    }
    if (Number.isNaN(amount) || amount < 0) {
      errors.push(`Line ${i + 1}: invalid amount`);
      continue;
    }
    entries.push({ conceptIndex: num - 1, amount });
  }

  return { entries, errors };
}

async function sendNextSeqPrompt(bot, chatId) {
  const state = stateByChat.get(chatId);
  if (!state || state.step !== 'seq') return;
  const spreadsheetId = getSpreadsheetId(state.user);
  if (!spreadsheetId) {
    await bot.sendMessage(chatId, 'Sheet not configured.');
    return;
  }
  let { conceptIndex, sessionEntries } = state;
  while (conceptIndex < CONCEPTS.length && CONCEPTS[conceptIndex].fixedAmount != null) {
    const c = CONCEPTS[conceptIndex];
    const row = buildRow(c, c.fixedAmount);
    try {
      await appendRow(spreadsheetId, rowToSheetRow(row));
      sessionEntries = [...sessionEntries, { label: c.label, amount: c.fixedAmount }];
      conceptIndex++;
      stateByChat.set(chatId, { ...state, conceptIndex, sessionEntries });
      await bot.sendMessage(chatId, `✅ ${c.fixedAmount.toLocaleString('es-ES')} € · ${c.label}`);
    } catch (err) {
      console.error('Sheets error:', err.message);
      await bot.sendMessage(chatId, 'Could not add ' + c.label);
      return;
    }
  }
  if (conceptIndex >= CONCEPTS.length) {
    stateByChat.set(chatId, { ...state, step: 'confirm', conceptIndex, sessionEntries });
    const summary = sessionEntries.map((e, i) => `${i + 1}. ${e.label}: ${Number(e.amount).toLocaleString('es-ES')} €`).join('\n');
    await bot.sendMessage(
      chatId,
      `You added:\n${summary}\n\nIs this correct? *Yes* to finish, *No* to edit (add more or remove last entry).`,
      { parse_mode: 'Markdown', ...confirmKeyboard }
    );
    return;
  }
  const c = CONCEPTS[conceptIndex];
  await bot.sendMessage(
    chatId,
    `*${conceptIndex + 1}/14* · ${c.label}\n💰 Amount? (€) or *skip* to omit.`,
    { parse_mode: 'Markdown', ...keyboardWithFi }
  );
}

const CATEGORY_ALIAS = {
  'caixa d\'estalvis': 'Cuenta ahorro',
  'caixa estalvis': 'Cuenta ahorro',
  'compte corrent': 'Cuenta corriente',
  'compte corrent caixabank': 'Cuenta corriente',
  'cuenta ahorro': 'Cuenta ahorro',
  'cuenta corriente': 'Cuenta corriente',
  'cash': 'Cash',
  'accions': 'Acciones',
  'acciones': 'Acciones',
  'cryptos': 'Cryptos',
  'cuenta compartida flexible': 'Cuenta compartida flexible',
  'compte compartit flexible': 'Cuenta compartida flexible',
  'cuenta flexible': 'Cuenta flexible',
  'compte flexible': 'Cuenta flexible',
  'crowfunding': 'Crowfunding',
  'fondo indexado': 'Fondo indexado',
  'fons indexat': 'Fondo indexado',
  'plan de pensiones': 'Plan de pensiones',
  'pla de pensions': 'Plan de pensiones',
  'hipoteca': 'Hipoteca',
};

const ENTITY_ALIAS = {
  'caixabank': 'CaixaBank',
  'revolut': 'Revolut',
  'efectiu': 'Efectivo',
  'efectivo': 'Efectivo',
  'trade republic': 'Trade Republic',
  'fundeen': 'Fundeen',
  'urbanitae': 'Urbanitae',
  'indexa capital': 'Indexa Capital',
  'bbva': 'BBVA',
};

function normalizeCategory(text) {
  const k = (text || '').toLowerCase().trim();
  return CATEGORY_ALIAS[k] || text.trim();
}

function normalizeEntity(text) {
  const k = (text || '').toLowerCase().trim();
  return ENTITY_ALIAS[k] || text.trim();
}

function getSpreadsheetId(user) {
  const id = user === USER_1 ? spreadsheetId1 : user === USER_2 ? spreadsheetId2 : null;
  return id && String(id).trim() ? id : null;
}

function getExcelLinkMessage(userId) {
  const spreadsheetId = getSpreadsheetId(userId);
  if (!spreadsheetId) return null;
  const url = `https://docs.google.com/spreadsheets/d/${spreadsheetId}/edit`;
  const label = `Finances personals ${getDisplayName(userId)}`;
  return `Revisa l'Excel: [${label}](${url})`;
}

function resetChat(chatId) {
  stateByChat.delete(chatId);
  userByChat.delete(chatId);
}

function clearState(chatId) {
  stateByChat.delete(chatId);
}

const whoKeyboard = {
  reply_markup: {
    keyboard: [['Olga'], ['Andrea']],
    resize_keyboard: true,
    one_time_keyboard: false,
  },
};

const keyboardWithFi = {
  reply_markup: {
    keyboard: [['Olga'], ['Andrea'], ['Done', 'Exit']],
    resize_keyboard: true,
    one_time_keyboard: false,
  },
};

const confirmKeyboard = {
  reply_markup: {
    keyboard: [['Yes'], ['No (edit)']],
    resize_keyboard: true,
    one_time_keyboard: false,
  },
};

const overwriteKeyboard = {
  reply_markup: {
    keyboard: [['Yes'], ['No']],
    resize_keyboard: true,
    one_time_keyboard: false,
  },
};

function monthLabel(month, year) {
  return `${new Date(2000, month - 1, 1).toLocaleString('en', { month: 'long' })} ${year}`;
}

const editKeyboard = {
  reply_markup: {
    keyboard: [['Another'], ['Last'], ['Done', 'Exit']],
    resize_keyboard: true,
    one_time_keyboard: false,
  },
};

function main() {
  if (!process.env.TELEGRAM_BOT_TOKEN) {
    console.error('Missing TELEGRAM_BOT_TOKEN in .env');
    process.exit(1);
  }
  if (!spreadsheetId1 || !spreadsheetId2) {
    console.error('Missing SPREADSHEET_ID and/or SPREADSHEET_ID_2 in .env');
    process.exit(1);
  }

  try {
    getSheetsClient();
  } catch (e) {
    console.error('Google Sheets:', e.message);
    process.exit(1);
  }

  const bot = createBot();

  bot.onText(/\/start/, async (msg) => {
    const chatId = msg.chat.id;
    resetChat(chatId);
    await bot.sendMessage(chatId, getWelcomeMessage(), whoKeyboard);
  });

  bot.onText(/\/help/, async (msg) => {
    const chatId = msg.chat.id;
    await bot.sendMessage(
      chatId,
      `Press *Olga* or *Andrea*. Then I ask for *one message* with all amounts: one line per concept, *N. amount* (€). Example: 1. 500, 2. 400… Vivienda personal (s’afegeix 150 000 €). Date: 1st of current month.\n\nWhen you finish (*Done*) I show the summary and ask if correct or you want to edit (add more or remove last entry).\n\n*Reset:* *stop* or /stop.`,
      { parse_mode: 'Markdown' }
    );
  });

  bot.onText(/\/stop/, async (msg) => {
    const chatId = msg.chat.id;
    resetChat(chatId);
    await bot.sendMessage(chatId, getResetMessage(), whoKeyboard);
  });

  bot.on('message', async (msg) => {
    const updateId = msg.update_id;
    if (updateId != null && wasProcessed(updateId)) return;

    const text = (msg.text || '').trim();
    const chatId = msg.chat.id;
    if (!text || text.startsWith('/')) return;

    const name = text.toLowerCase();
    if (name === 'sortir' || name === 'exit') {
      resetChat(chatId);
      await bot.sendMessage(chatId, getResetMessage(), whoKeyboard);
      return;
    }
    const userId = nameToUserId(name);
    if (userId !== null) {
      userByChat.set(chatId, userId);
      clearState(chatId);
      stateByChat.set(chatId, { user: userId, step: 'bulk', sessionEntries: [] });
      const listMessage = userId === USER_1 ? OLGA_ENTITIES_MESSAGE : userId === USER_2 ? ANDREA_ENTITIES_MESSAGE : 'Press *Olga* or *Andrea* to start.';
      await bot.sendMessage(
        chatId,
        `Hi ${getDisplayName(userId)} 👋\n\n${listMessage}`,
        { parse_mode: 'Markdown', ...keyboardWithFi }
      );
      return;
    }

    if (name === 'fi' || name === 'no' || name === 'acabar' || name === 'stop' || name === 'done' || name === 'Done') {
      const state = stateByChat.get(chatId);
      const user = userByChat.get(chatId);
      if (user && state?.sessionEntries?.length > 0) {
        stateByChat.set(chatId, { ...state, step: 'confirm' });
        const summary = state.sessionEntries.map((e, i) => `${i + 1}. ${e.label}: ${Number(e.amount).toLocaleString('es-ES')} €`).join('\n');
        await bot.sendMessage(
          chatId,
          `You added:\n${summary}\n\nIs this correct? *Yes* to finish, *No* to edit (add more or remove last entry).`,
          { parse_mode: 'Markdown', ...confirmKeyboard }
        );
        return;
      }
      resetChat(chatId);
      await bot.sendMessage(chatId, getResetMessage(), whoKeyboard);
      return;
    }

    const user = userByChat.get(chatId);
    if (!user) {
      await bot.sendMessage(chatId, getWelcomeMessage(), whoKeyboard);
      return;
    }

    let state = stateByChat.get(chatId);
    if (!state) {
      state = { user, step: user === USER_1 || user === USER_2 ? 'bulk' : 'concept', sessionEntries: [] };
      stateByChat.set(chatId, state);
    }

    if (state.step === 'concept' && (state.user === USER_2 || state.user === USER_1)) {
      const labelResult = state.user === USER_2 ? parseAndreaLabelMessage(text) : parseOlgaLabelMessage(text);
      if (labelResult.entries.length > 0) {
        state = { ...state, step: 'bulk' };
        stateByChat.set(chatId, state);
      }
    }

    if (state.step === 'bulk') {
      const spreadsheetId = getSpreadsheetId(state.user);
      if (!spreadsheetId) {
        const whoName = state.user === USER_2 ? 'Andrea' : state.user === USER_1 ? 'Olga' : '';
        await bot.sendMessage(chatId, whoName ? `Sheet for ${whoName} not configured. Check SPREADSHEET_ID${state.user === USER_2 ? '_2' : ''} in .env` : 'Sheet not configured.');
        return;
      }
      const concepts = getConceptsForUser(state.user);
      let result;
      if (state.user === USER_1) {
        const labelResult = parseOlgaLabelMessage(text);
        result =
          labelResult.entries.length > 0
            ? labelResult
            : labelResult.errors.length > 0
              ? labelResult
              : parseBulkMessage(text);
      } else if (state.user === USER_2) {
        const labelResult = parseAndreaLabelMessage(text);
        result =
          labelResult.entries.length > 0
            ? labelResult
            : labelResult.errors.length > 0
              ? labelResult
              : parseBulkMessage(text, ANDREA_CONCEPTS);
      } else {
        result = parseBulkMessage(text);
      }
      const { entries, errors } = result;
      if (entries.length === 0) {
        const errorDetail =
          errors.length > 0
            ? `❌ What failed:\n${errors.join('\n')}\n\nCheck that each line is *Label: amount* (e.g. Compte corrent (BBVA): 100). Make sure you chose who you are (Andrea/Olga) correctly.`
            : 'No valid lines found. Use *Label: amount* (e.g. Efectiu: 10) or *N. amount* (e.g. 1. 500).';
        await bot.sendMessage(chatId, errorDetail, { parse_mode: 'Markdown', ...keyboardWithFi });
        return;
      }
      let entriesToWrite = [...entries].sort((a, b) => a.conceptIndex - b.conceptIndex);
      if (state.user === USER_1 && !entriesToWrite.some((e) => e.conceptIndex === 12)) {
        entriesToWrite.push({ conceptIndex: 12, amount: 150000 });
        entriesToWrite.sort((a, b) => a.conceptIndex - b.conceptIndex);
      }
      if (state.user === USER_2 && !entriesToWrite.some((e) => e.conceptIndex === 8)) {
        entriesToWrite.push({ conceptIndex: 8, amount: 150000 });
        entriesToWrite.sort((a, b) => a.conceptIndex - b.conceptIndex);
      }
      const base = firstDayOfMonthRow();
      const now = new Date();
      const isDay1 = now.getDate() === 1;
      let existingIndices = [];
      try {
        existingIndices = await getRowIndicesForMonth(spreadsheetId, base.month, base.year);
      } catch (err) {
        console.error('Error checking existing month data:', err.message);
      }
      const writeBase = isDay1 ? base : (existingIndices.length > 0 ? nextMonthFrom(base) : base);
      const prev = previousMonth(writeBase);
      let existingPrev = [];
      let existingForWrite = [];
      try {
        existingPrev = await getRowIndicesForMonth(spreadsheetId, prev.month, prev.year);
        existingForWrite = await getRowIndicesForMonth(spreadsheetId, writeBase.month, writeBase.year);
      } catch (err) {
        console.error('Error checking month data:', err.message);
      }
      if (existingPrev.length > 0 && existingForWrite.length === 0) {
        const prevMonthName = new Date(2000, prev.month - 1, 1).toLocaleString('en', { month: 'long' });
        const destMonthName = new Date(2000, writeBase.month - 1, 1).toLocaleString('en', { month: 'long' });
        stateByChat.set(chatId, {
          ...state,
          step: 'confirm_copy_previous',
          pendingBulkEntries: entriesToWrite,
          pendingBulkErrors: errors,
          pendingMonth: writeBase.month,
          pendingYear: writeBase.year,
          pendingPrevMonth: prev.month,
          pendingPrevYear: prev.year,
        });
        await bot.sendMessage(
          chatId,
          `There is data for *${prevMonthName} ${prev.year}*. Do you want to copy it to *${destMonthName} ${writeBase.year}*?`,
          { parse_mode: 'Markdown', ...overwriteKeyboard }
        );
        return;
      }
      if (existingForWrite.length > 0) {
        const nextBase = nextMonthFrom(writeBase);
        let existingNext = [];
        try {
          existingNext = await getRowIndicesForMonth(spreadsheetId, nextBase.month, nextBase.year);
        } catch (err) {
          console.error('Error checking next month:', err.message);
        }
        if (existingNext.length > 0) {
          const label1 = monthLabel(writeBase.month, writeBase.year);
          const label2 = monthLabel(nextBase.month, nextBase.year);
          stateByChat.set(chatId, {
            ...state,
            step: 'choose_month_replace',
            pendingBulkEntries: entriesToWrite,
            pendingBulkErrors: errors,
            replaceOptions: [
              { dateStr: writeBase.dateStr, month: writeBase.month, year: writeBase.year, label: label1 },
              { dateStr: nextBase.dateStr, month: nextBase.month, year: nextBase.year, label: label2 },
            ],
          });
          const replaceKeyboard = {
            reply_markup: {
              keyboard: [[label1], [label2], ['No']],
              resize_keyboard: true,
              one_time_keyboard: true,
            },
          };
          await bot.sendMessage(
            chatId,
            `There is already data for *${label1}* and *${label2}*. Do you want to replace a month? Choose which:`,
            { parse_mode: 'Markdown', ...replaceKeyboard }
          );
          return;
        }
        const monthName = monthLabel(writeBase.month, writeBase.year);
        const nextMonthName = monthLabel(nextBase.month, nextBase.year);
        await bot.sendMessage(
          chatId,
          `There is already data for *${monthName}*. I will add to the next month (*${nextMonthName}*).`,
          { parse_mode: 'Markdown' }
        );
        const sessionEntries = [];
        for (const e of entriesToWrite) {
          const c = concepts[e.conceptIndex];
          let amount = e.amount;
          if (e.conceptIndex === 3) {
            amount = amount / 2;
          }
          if (state.user === USER_1 && e.conceptIndex === 13) {
            amount = amount / 2;
            if (amount > 0) amount = -amount;
          }
          if (state.user === USER_2 && e.conceptIndex === 9) {
            amount = amount / 2;
            if (amount > 0) amount = -amount;
          }
          const row = buildRow(c, amount, nextBase);
          try {
            await appendRow(spreadsheetId, rowToSheetRow(row));
            const isAutoViviendaOlga = state.user === USER_1 && e.conceptIndex === 12 && amount === 150000;
            const isAutoViviendaAndrea = state.user === USER_2 && e.conceptIndex === 8 && amount === 150000;
            if (!isAutoViviendaOlga && !isAutoViviendaAndrea) {
              const displayLabel = getSummaryLabel(state.user, e.conceptIndex);
              sessionEntries.push({ label: displayLabel, amount });
            }
          } catch (err) {
            console.error('Sheets error:', err.message);
            const whoName = state.user === USER_2 ? 'Andrea' : 'Olga';
            await bot.sendMessage(chatId, `Could not add ${c.label} to ${whoName}'s sheet. Error: ${err.message}`);
            return;
          }
        }
        stateByChat.set(chatId, { ...state, step: 'confirm', sessionEntries });
        const summary = sessionEntries.map((s, i) => `${i + 1}. ${s.label}: ${Number(s.amount).toLocaleString('es-ES')} €`).join('\n');
        let confirmText = `Added to next month ✅\n${summary}\n\nIs this correct? *Yes* to finish, *No* to edit.`;
        if (errors.length > 0) confirmText += `\n\n⚠️ No reconeguts: ${errors.join('; ')}`;
        await bot.sendMessage(chatId, confirmText, { parse_mode: 'Markdown', ...confirmKeyboard });
        return;
      }
      const sessionEntries = [];
      for (const e of entriesToWrite) {
        const c = concepts[e.conceptIndex];
        let amount = e.amount;
        if (e.conceptIndex === 3) {
          amount = amount / 2;
        }
        if (state.user === USER_1 && e.conceptIndex === 13) {
          amount = amount / 2;
          if (amount > 0) amount = -amount;
        }
        if (state.user === USER_2 && e.conceptIndex === 9) {
          amount = amount / 2;
          if (amount > 0) amount = -amount;
        }
                const row = buildRow(c, amount, writeBase);
        try {
          await appendRow(spreadsheetId, rowToSheetRow(row));
          const isAutoViviendaOlga = state.user === USER_1 && e.conceptIndex === 12 && amount === 150000;
          const isAutoViviendaAndrea = state.user === USER_2 && e.conceptIndex === 8 && amount === 150000;
          if (!isAutoViviendaOlga && !isAutoViviendaAndrea) {
            const displayLabel = getSummaryLabel(state.user, e.conceptIndex);
            sessionEntries.push({ label: displayLabel, amount });
          }
        } catch (err) {
          console.error('Sheets error:', err.message);
          const whoName = state.user === USER_2 ? 'Andrea' : 'Olga';
          await bot.sendMessage(chatId, `Could not add ${c.label} to ${whoName}'s sheet. Error: ${err.message}`);
          return;
        }
      }
      stateByChat.set(chatId, { ...state, step: 'confirm', sessionEntries });
      const summary = sessionEntries.map((s, i) => `${i + 1}. ${s.label}: ${Number(s.amount).toLocaleString('es-ES')} €`).join('\n');
      const noDataMsg = 'There was no data for this month; it has been added.';
      let confirmText = `${noDataMsg}\n\nAdded ✅\n${summary}\n\nIs this correct? *Yes* to finish, *No* to edit.`;
      if (errors.length > 0) confirmText += `\n\n⚠️ No reconeguts: ${errors.join('; ')}`;
      await bot.sendMessage(chatId, confirmText, { parse_mode: 'Markdown', ...confirmKeyboard });
      return;
    }

    if (state.step === 'seq') {
      const spreadsheetId = getSpreadsheetId(state.user);
      if (!spreadsheetId) {
        await bot.sendMessage(chatId, 'Sheet not configured.');
        return;
      }
      const isSkip = /^(salta|skip|omitir|omite|-)$/i.test(name);
      if (isSkip) {
        stateByChat.set(chatId, { ...state, conceptIndex: state.conceptIndex + 1 });
        await sendNextSeqPrompt(bot, chatId);
        return;
      }
      const amount = parseFloat(text.replace(',', '.'));
      if (Number.isNaN(amount)) {
        const c = CONCEPTS[state.conceptIndex];
        await bot.sendMessage(chatId, `Enter an amount (€) or *skip* to omit.\n_${c.label}_`, { parse_mode: 'Markdown', ...keyboardWithFi });
        return;
      }
      const c = CONCEPTS[state.conceptIndex];
      let amountToWrite = amount;
      if (state.conceptIndex === 3) {
        amountToWrite = amount / 2;
      }
      if (state.user === USER_1 && state.conceptIndex === 13) {
        amountToWrite = amountToWrite / 2;
        if (amountToWrite > 0) amountToWrite = -amountToWrite;
      }
      const row = buildRow(c, amountToWrite);
      try {
        await appendRow(spreadsheetId, rowToSheetRow(row));
        const newEntries = [...(state.sessionEntries || []), { label: c.label, amount: amountToWrite }];
        stateByChat.set(chatId, { ...state, conceptIndex: state.conceptIndex + 1, sessionEntries: newEntries });
        await bot.sendMessage(chatId, `✅ ${amountToWrite} € · ${c.label}`);
        await sendNextSeqPrompt(bot, chatId);
      } catch (err) {
        console.error('Sheets error:', err.message);
        await bot.sendMessage(chatId, 'Could not add.', keyboardWithFi);
      }
      return;
    }

    if (state.step === 'concept') {
      const concept = getConceptById(text);
      if (!concept) {
        await bot.sendMessage(chatId, `Enter a number from 1 to ${CONCEPTS.length}:\n${conceptsListMessage()}`, keyboardWithFi);
        return;
      }

      const spreadsheetId = getSpreadsheetId(state.user);
      if (!spreadsheetId) {
        await bot.sendMessage(chatId, 'Sheet not configured.');
        clearState(chatId);
        return;
      }

      if (concept.fixedAmount != null) {
        const row = buildRow(concept, concept.fixedAmount);
        try {
          await appendRow(spreadsheetId, rowToSheetRow(row));
          const newEntries = [...(state.sessionEntries || []), { label: concept.label, amount: concept.fixedAmount }];
          stateByChat.set(chatId, { ...state, sessionEntries: newEntries });
          await bot.sendMessage(chatId, `Added ✅ ${concept.fixedAmount.toLocaleString('es-ES')} € · ${concept.label}`);
        } catch (err) {
          console.error('Sheets error:', err.message);
          await bot.sendMessage(chatId, 'Could not add.');
        }
        await bot.sendMessage(chatId, 'Another concept? (enter the number or *Done* to finish)', { parse_mode: 'Markdown', ...keyboardWithFi });
        return;
      }

      stateByChat.set(chatId, { ...state, step: 'amount', concept });
      await bot.sendMessage(chatId, `💰 Amount? (€)\n_${concept.label}_`, { parse_mode: 'Markdown', ...keyboardWithFi });
      return;
    }

    if (state.step === 'amount') {
      const amount = parseFloat(text.replace(',', '.'));
      if (Number.isNaN(amount)) {
        await bot.sendMessage(chatId, 'The amount must be a number. Example: 8859', keyboardWithFi);
        return;
      }

      const spreadsheetId = getSpreadsheetId(state.user);
      if (!spreadsheetId) {
        await bot.sendMessage(chatId, 'Sheet not configured.');
        clearState(chatId);
        return;
      }

      const row = buildRow(state.concept, amount);
      try {
        await appendRow(spreadsheetId, rowToSheetRow(row));
        const newEntries = [...(state.sessionEntries || []), { label: state.concept.label, amount }];
        stateByChat.set(chatId, { user: state.user, step: 'concept', sessionEntries: newEntries });
        await bot.sendMessage(chatId, `Added ✅ ${amount} € · ${state.concept.label}`);
      } catch (err) {
        console.error('Sheets error:', err.message);
        await bot.sendMessage(chatId, 'Could not add.');
      }

      await bot.sendMessage(chatId, 'Another concept? (enter the number or *Done* to finish)', { parse_mode: 'Markdown', ...keyboardWithFi });
    }

    if (state.step === 'confirm_copy_previous') {
      const copyYes = name === 'sí' || name === 'si' || name === 'sip' || name === 'yes';
      const copyNo = name === 'no';
      if (copyNo) {
        const writeBase = { dateStr: `01/${String(state.pendingMonth).padStart(2, '0')}/${state.pendingYear}`, month: state.pendingMonth, year: state.pendingYear };
        const spreadsheetId = getSpreadsheetId(state.user);
        const concepts = getConceptsForUser(state.user);
        const entriesToWrite = state.pendingBulkEntries || [];
        const errors = state.pendingBulkErrors || [];
        const sessionEntries = [];
        for (const e of entriesToWrite) {
          const c = concepts[e.conceptIndex];
          let amount = e.amount;
          if (e.conceptIndex === 3) amount = amount / 2;
          if (state.user === USER_1 && e.conceptIndex === 13) { amount = amount / 2; if (amount > 0) amount = -amount; }
          if (state.user === USER_2 && e.conceptIndex === 9) { amount = amount / 2; if (amount > 0) amount = -amount; }
          const row = buildRow(c, amount, writeBase);
          try {
            await appendRow(spreadsheetId, rowToSheetRow(row));
            const isAutoViviendaOlga = state.user === USER_1 && e.conceptIndex === 12 && amount === 150000;
            const isAutoViviendaAndrea = state.user === USER_2 && e.conceptIndex === 8 && amount === 150000;
            if (!isAutoViviendaOlga && !isAutoViviendaAndrea) sessionEntries.push({ label: getSummaryLabel(state.user, e.conceptIndex), amount });
          } catch (err) {
            console.error('Sheets error:', err.message);
            await bot.sendMessage(chatId, `Could not add: ${err.message}`);
            return;
          }
        }
        stateByChat.set(chatId, { ...state, step: 'confirm', sessionEntries });
        const summary = sessionEntries.map((s, i) => `${i + 1}. ${s.label}: ${Number(s.amount).toLocaleString('es-ES')} €`).join('\n');
        let confirmText = `There was no data for this month; it has been added.\n\nAdded ✅\n${summary}\n\nIs this correct? *Yes* to finish, *No* to edit.`;
        if (errors.length > 0) confirmText += `\n\n⚠️ No reconeguts: ${errors.join('; ')}`;
        await bot.sendMessage(chatId, confirmText, { parse_mode: 'Markdown', ...confirmKeyboard });
        return;
      }
      if (copyYes) {
        const spreadsheetId = getSpreadsheetId(state.user);
        if (!spreadsheetId) {
          await bot.sendMessage(chatId, 'Sheet not configured.');
          return;
        }
        try {
          await copyMonthToMonth(spreadsheetId, state.pendingPrevMonth, state.pendingPrevYear, state.pendingMonth, state.pendingYear);
        } catch (err) {
          console.error('Error copying month:', err.message);
          await bot.sendMessage(chatId, `Could not copy data: ${err.message}`);
          return;
        }
        const destMonthName = new Date(2000, state.pendingMonth - 1, 1).toLocaleString('en', { month: 'long' });
        const writeBase = { dateStr: `01/${String(state.pendingMonth).padStart(2, '0')}/${state.pendingYear}`, month: state.pendingMonth, year: state.pendingYear };
        const nextBase = nextMonthFrom(writeBase);
        const nextMonthName = new Date(2000, nextBase.month - 1, 1).toLocaleString('en', { month: 'long' });
        await bot.sendMessage(
          chatId,
          `Data was copied to *${destMonthName} ${state.pendingYear}*. There is already data there. I will add to the next month (*${nextMonthName} ${nextBase.year}*).`,
          { parse_mode: 'Markdown' }
        );
        const concepts = getConceptsForUser(state.user);
        const entriesToWrite = state.pendingBulkEntries || [];
        const errors = state.pendingBulkErrors || [];
        const sessionEntries = [];
        for (const e of entriesToWrite) {
          const c = concepts[e.conceptIndex];
          let amount = e.amount;
          if (e.conceptIndex === 3) amount = amount / 2;
          if (state.user === USER_1 && e.conceptIndex === 13) { amount = amount / 2; if (amount > 0) amount = -amount; }
          if (state.user === USER_2 && e.conceptIndex === 9) { amount = amount / 2; if (amount > 0) amount = -amount; }
          const row = buildRow(c, amount, nextBase);
          try {
            await appendRow(spreadsheetId, rowToSheetRow(row));
            const isAutoViviendaOlga = state.user === USER_1 && e.conceptIndex === 12 && amount === 150000;
            const isAutoViviendaAndrea = state.user === USER_2 && e.conceptIndex === 8 && amount === 150000;
            if (!isAutoViviendaOlga && !isAutoViviendaAndrea) sessionEntries.push({ label: getSummaryLabel(state.user, e.conceptIndex), amount });
          } catch (err) {
            console.error('Sheets error:', err.message);
            await bot.sendMessage(chatId, `Could not add: ${err.message}`);
            return;
          }
        }
        stateByChat.set(chatId, { ...state, step: 'confirm', sessionEntries, pendingPrevMonth: undefined, pendingPrevYear: undefined });
        const summary = sessionEntries.map((s, i) => `${i + 1}. ${s.label}: ${Number(s.amount).toLocaleString('es-ES')} €`).join('\n');
        let confirmText = `Added to next month ✅\n${summary}\n\nIs this correct? *Yes* to finish, *No* to edit.`;
        if (errors.length > 0) confirmText += `\n\n⚠️ No reconeguts: ${errors.join('; ')}`;
        await bot.sendMessage(chatId, confirmText, { parse_mode: 'Markdown', ...confirmKeyboard });
        return;
      }
      await bot.sendMessage(chatId, 'Reply *Yes* (copy) or *No* (add only the new amounts).', { parse_mode: 'Markdown', ...overwriteKeyboard });
      return;
    }

    if (state.step === 'choose_month_replace') {
      const reply = (name || '').trim().toLowerCase();
      if (reply === 'no') {
        resetChat(chatId);
        await bot.sendMessage(chatId, 'OK, nothing was replaced.\n\n' + getResetMessage(), { parse_mode: 'Markdown', ...whoKeyboard });
        return;
      }
      const options = state.replaceOptions || [];
      const chosen = options.find((opt) => opt.label.trim().toLowerCase() === reply);
      if (!chosen) {
        const labels = options.map((o) => o.label).join(' o ');
        await bot.sendMessage(chatId, `Choose a month: *${labels}*, or *No* to not replace.`, { parse_mode: 'Markdown' });
        return;
      }
      const spreadsheetId = getSpreadsheetId(state.user);
      if (!spreadsheetId) {
        await bot.sendMessage(chatId, 'Sheet not configured.');
        return;
      }
      const concepts = getConceptsForUser(state.user);
      const entriesToWrite = state.pendingBulkEntries || [];
      const errors = state.pendingBulkErrors || [];
      let rowIndices = [];
      try {
        rowIndices = await getRowIndicesForMonth(spreadsheetId, chosen.month, chosen.year);
      } catch (err) {
        console.error('Error getting row indices:', err.message);
        await bot.sendMessage(chatId, `Could not read sheet data: ${err.message}`);
        return;
      }
      try {
        await deleteRows(spreadsheetId, rowIndices);
      } catch (err) {
        console.error('Error deleting rows:', err.message);
        await bot.sendMessage(chatId, `Could not delete rows: ${err.message}`);
        return;
      }
      const replaceBase = { dateStr: chosen.dateStr, month: chosen.month, year: chosen.year };
      const sessionEntries = [];
      for (const e of entriesToWrite) {
        const c = concepts[e.conceptIndex];
        let amount = e.amount;
        if (e.conceptIndex === 3) amount = amount / 2;
        if (state.user === USER_1 && e.conceptIndex === 13) { amount = amount / 2; if (amount > 0) amount = -amount; }
        if (state.user === USER_2 && e.conceptIndex === 9) { amount = amount / 2; if (amount > 0) amount = -amount; }
        const row = buildRow(c, amount, replaceBase);
        try {
          await appendRow(spreadsheetId, rowToSheetRow(row));
          const isAutoViviendaOlga = state.user === USER_1 && e.conceptIndex === 12 && amount === 150000;
          const isAutoViviendaAndrea = state.user === USER_2 && e.conceptIndex === 8 && amount === 150000;
          if (!isAutoViviendaOlga && !isAutoViviendaAndrea) sessionEntries.push({ label: getSummaryLabel(state.user, e.conceptIndex), amount });
        } catch (err) {
          console.error('Sheets error:', err.message);
          await bot.sendMessage(chatId, `Could not add: ${err.message}`);
          return;
        }
      }
      stateByChat.set(chatId, { ...state, step: 'confirm', sessionEntries, replaceOptions: undefined });
      const summary = sessionEntries.map((s, i) => `${i + 1}. ${s.label}: ${Number(s.amount).toLocaleString('es-ES')} €`).join('\n');
      let confirmText = `Replaced *${chosen.label}* ✅\n${summary}\n\nIs this correct? *Yes* to finish, *No* to edit.`;
      if (errors.length > 0) confirmText += `\n\n⚠️ No reconeguts: ${errors.join('; ')}`;
      await bot.sendMessage(chatId, confirmText, { parse_mode: 'Markdown', ...confirmKeyboard });
      return;
    }

    if (state.step === 'confirm_overwrite') {
      const overwriteYes = name === 'sí' || name === 'si' || name === 'sip' || name === 'yes';
      const overwriteNo = name === 'no';
      if (overwriteNo) {
        resetChat(chatId);
        await bot.sendMessage(
          chatId,
          'OK, nothing was overwritten.\n\n' + getResetMessage(),
          { parse_mode: 'Markdown', ...whoKeyboard }
        );
        return;
      }
      if (overwriteYes) {
        const spreadsheetId = getSpreadsheetId(state.user);
        if (!spreadsheetId) {
          await bot.sendMessage(chatId, 'Sheet not configured.');
          return;
        }
        let rowIndices = [];
        try {
          rowIndices = await getRowIndicesForMonth(spreadsheetId, state.pendingMonth, state.pendingYear);
        } catch (err) {
          console.error('Error getting row indices for overwrite:', err.message);
          await bot.sendMessage(chatId, `Could not read sheet data: ${err.message}`);
          return;
        }
        if (rowIndices.length > 0) {
          try {
            await deleteRows(spreadsheetId, rowIndices);
          } catch (err) {
            console.error('Error deleting rows:', err.message);
            await bot.sendMessage(chatId, `Could not delete rows: ${err.message}`);
            return;
          }
        }
        const concepts = getConceptsForUser(state.user);
        const entriesToWrite = state.pendingBulkEntries || [];
        const errors = state.pendingBulkErrors || [];
        const pendingBase = {
          dateStr: `01/${String(state.pendingMonth).padStart(2, '0')}/${state.pendingYear}`,
          month: state.pendingMonth,
          year: state.pendingYear,
        };
        const sessionEntries = [];
        for (const e of entriesToWrite) {
          const c = concepts[e.conceptIndex];
          let amount = e.amount;
          if (e.conceptIndex === 3) amount = amount / 2;
          if (state.user === USER_1 && e.conceptIndex === 13) {
            amount = amount / 2;
            if (amount > 0) amount = -amount;
          }
          if (state.user === USER_2 && e.conceptIndex === 9) {
            amount = amount / 2;
            if (amount > 0) amount = -amount;
          }
          const row = buildRow(c, amount, pendingBase);
          try {
            await appendRow(spreadsheetId, rowToSheetRow(row));
            const isAutoViviendaOlga = state.user === USER_1 && e.conceptIndex === 12 && amount === 150000;
            const isAutoViviendaAndrea = state.user === USER_2 && e.conceptIndex === 8 && amount === 150000;
            if (!isAutoViviendaOlga && !isAutoViviendaAndrea) {
              sessionEntries.push({ label: getSummaryLabel(state.user, e.conceptIndex), amount });
            }
          } catch (err) {
            console.error('Sheets error:', err.message);
            const whoName = state.user === USER_2 ? 'Andrea' : 'Olga';
            await bot.sendMessage(chatId, `Could not add ${c.label} to ${whoName}'s sheet. Error: ${err.message}`);
            return;
          }
        }
        stateByChat.set(chatId, { ...state, step: 'confirm', sessionEntries });
        const monthName = new Date(2000, state.pendingMonth - 1, 1).toLocaleString('en', { month: 'long' });
        const summary = sessionEntries.map((s, i) => `${i + 1}. ${s.label}: ${Number(s.amount).toLocaleString('es-ES')} €`).join('\n');
        let confirmText = `Replaced data for *${monthName} ${state.pendingYear}*.\n\nUpdated ✅\n${summary}\n\nIs this correct? *Yes* to finish, *No* to edit.`;
        if (errors.length > 0) confirmText += `\n\n⚠️ No reconeguts: ${errors.join('; ')}`;
        await bot.sendMessage(chatId, confirmText, { parse_mode: 'Markdown', ...confirmKeyboard });
        return;
      }
      await bot.sendMessage(
        chatId,
        'Choose an option:\n\n*Yes* → I replace the sheet data with the amounts you sent.\n*No* → I change nothing and reset.',
        { parse_mode: 'Markdown', ...overwriteKeyboard }
      );
      return;
    }

    if (state.step === 'confirm') {
      const ok = name === 'sí' || name === 'si' || name === 'correcte' || name === 'ok' || name === 'sip' || name === 'yes' || name === 'Yes';
      const no = name === 'no' || name === 'editar' || name === 'no (editar)' || name === 'no (edit)' || name.startsWith('no ');
      if (ok) {
        const userId = userByChat.get(chatId);
        const excelLink = getExcelLinkMessage(userId);
        resetChat(chatId);
        const endMsg = excelLink
          ? `${excelLink}\n\n${getResetMessage()}`
          : getResetMessage();
        await bot.sendMessage(chatId, endMsg, { parse_mode: 'Markdown', ...whoKeyboard });
        return;
      }
      if (no) {
        stateByChat.set(chatId, { ...state, step: 'edit' });
        await bot.sendMessage(
          chatId,
          'Type *Another* to add more concepts, or *Last* to remove the last entry.',
          { parse_mode: 'Markdown', ...editKeyboard }
        );
        return;
      }
      await bot.sendMessage(chatId, 'Reply *Yes* or *No (edit)*.', { parse_mode: 'Markdown', ...confirmKeyboard });
      return;
    }

    if (state.step === 'edit') {
      if (name === 'altre' || name === 'another') {
        stateByChat.set(chatId, { ...state, step: 'concept' });
        await bot.sendMessage(
          chatId,
          `Choose concept (enter the number):\n${conceptsListMessage()}`,
          { parse_mode: 'Markdown', ...keyboardWithFi }
        );
        return;
      }
      if (name === 'darrera' || name === 'last') {
        const spreadsheetId = getSpreadsheetId(state.user);
        if (spreadsheetId && state.sessionEntries?.length > 0) {
          try {
            await clearLastRow(spreadsheetId);
            const prevEntries = state.sessionEntries.slice(0, -1);
            stateByChat.set(chatId, { ...state, sessionEntries: prevEntries, step: 'concept' });
            await bot.sendMessage(chatId, 'Last entry deleted.', keyboardWithFi);
            await bot.sendMessage(chatId, 'Another concept? (enter the number or *Done* to finish)', { parse_mode: 'Markdown', ...keyboardWithFi });
          } catch (err) {
            console.error('Sheets error:', err.message);
            await bot.sendMessage(chatId, 'Could not delete.', editKeyboard);
          }
        } else {
          await bot.sendMessage(chatId, 'No entries to remove.', editKeyboard);
        }
        return;
      }
      if (name === 'fi' || name === 'done') {
        const prevEntries = state.sessionEntries || [];
        const summary = prevEntries.map((e, i) => `${i + 1}. ${e.label}: ${Number(e.amount).toLocaleString('es-ES')} €`).join('\n');
        if (!summary) {
          resetChat(chatId);
          await bot.sendMessage(chatId, 'No entries. ' + getWelcomeMessage(), whoKeyboard);
          return;
        }
        stateByChat.set(chatId, { ...state, step: 'confirm' });
        await bot.sendMessage(chatId, `Has afegit:\n${summary}\n\nIs this correct? *Yes* to finish, *No* to edit.`, { parse_mode: 'Markdown', ...confirmKeyboard });
        return;
      }
      await bot.sendMessage(chatId, 'Type *Another*, *Last* or *Done*.', { parse_mode: 'Markdown', ...editKeyboard });
      return;
    }
  });

  bot.onText(/\/(o|a)\s*(.*)/, async (msg, match) => {
    const cmd = match[1];
    const text = (match[2] || '').trim();
    const chatId = msg.chat.id;
    const user = cmd === 'o' ? USER_1 : USER_2;
    userByChat.set(chatId, user);

    const spreadsheetId = getSpreadsheetId(user);
    if (!spreadsheetId) return;

    if (!text) {
      stateByChat.set(chatId, { user, step: 'concept' });
      await bot.sendMessage(
        chatId,
        `Choose concept (enter the number):\n${conceptsListMessage()}`,
        { parse_mode: 'Markdown', ...keyboardWithFi }
      );
      return;
    }

    const amount = parseFloat(text.replace(',', '.'));
    if (!Number.isNaN(amount)) {
      const indexaConcept = CONCEPTS[10];
      const row = buildRow(indexaConcept, amount);
      try {
        await appendRow(spreadsheetId, rowToSheetRow(row));
        await bot.sendMessage(chatId, `Added ✅ ${amount} € · ${indexaConcept.label}`);
      } catch (err) {
        await bot.sendMessage(chatId, 'Could not add.');
      }
      return;
    }

    const parsed = parseMessage(text);
    if (parsed.error) {
      await bot.sendMessage(chatId, parsed.error);
      return;
    }

    try {
      await appendRow(spreadsheetId, rowToSheetRow(parsed));
      await bot.sendMessage(chatId, `Added: ${parsed.amount} € · ${parsed.conceptLabel}`);
    } catch (err) {
      console.error('Sheets error:', err.message);
      await bot.sendMessage(chatId, 'Could not add.');
    }
  });

  bot.on('polling_error', (err) => {
    const is409 = err.message && String(err.message).includes('409');
    if (is409) {
      console.error('409 Conflict: another bot instance may be running. Wait a few seconds and leave only one. Retrying in 10 s...');
      bot.stopPolling().catch(() => {});
      setTimeout(() => {
        bot.startPolling().catch((e) => console.error('Error en reprendre polling:', e.message));
      }, 10000);
    } else {
      console.error('Polling error:', err.message);
    }
  });

  bot.startPolling().catch((e) => {
    console.error('Error iniciant polling:', e.message);
    process.exit(1);
  });
  console.log('Bot running.');
}

main();
