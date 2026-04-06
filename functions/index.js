const admin = require('firebase-admin');
const { onCall, HttpsError } = require('firebase-functions/v2/https');
const { defineSecret } = require('firebase-functions/params');
const OpenAI = require('openai');

admin.initializeApp();

const OPENAI_API_KEY = defineSecret('OPENAI_API_KEY');

function getStringParam(template, key, fallback) {
  const p = template?.parameters?.[key];
  const v = p?.defaultValue?.value;
  if (typeof v === 'string' && v.trim()) return v.trim();
  return fallback;
}

function getBoolParam(template, key, fallback) {
  const p = template?.parameters?.[key];
  const raw = p?.defaultValue?.value;
  if (typeof raw === 'boolean') return raw;
  if (typeof raw === 'string') {
    const s = raw.trim().toLowerCase();
    if (s === 'true') return true;
    if (s === 'false') return false;
  }
  return fallback;
}

async function loadAiSettingsFromRemoteConfig() {
  try {
    const template = await admin.remoteConfig().getTemplate();
    const enabled = getBoolParam(template, 'ai_secondary_enabled', false);
    const model = getStringParam(template, 'ai_openai_model', 'gpt-4o-mini');
    return { enabled, model };
  } catch (e) {
    return { enabled: false, model: 'gpt-4o-mini' };
  }
}

exports.aiProxy = onCall(
  {
    region: 'us-central1',
    cors: true,
    secrets: [OPENAI_API_KEY],
    timeoutSeconds: 25,
    memory: '256MiB',
  },
  async (request) => {
    const data = request.data || {};
    const task = (data.task || '').toString();

    if (task !== 'chat') {
      throw new HttpsError('invalid-argument', 'unsupported task');
    }

    const prompt = (data.prompt || '').toString();
    if (!prompt.trim()) {
      throw new HttpsError('invalid-argument', 'missing prompt');
    }

    const temperature = Number.isFinite(Number(data.temperature))
      ? Number(data.temperature)
      : 0.7;
    const maxOutputTokens = Number.isFinite(Number(data.maxOutputTokens))
      ? Number(data.maxOutputTokens)
      : 256;

    const settings = await loadAiSettingsFromRemoteConfig();
    if (!settings.enabled) {
      return {
        status: 403,
        content: '',
        raw: 'secondary_disabled',
      };
    }

    const client = new OpenAI({ apiKey: OPENAI_API_KEY.value() });

    try {
      const resp = await client.chat.completions.create({
        model: settings.model,
        temperature,
        max_tokens: maxOutputTokens,
        messages: [{ role: 'user', content: prompt }],
      });

      const content = (resp.choices?.[0]?.message?.content || '').trim();
      return {
        status: 200,
        content,
        raw: JSON.stringify(resp).slice(0, 4000),
        model: settings.model,
      };
    } catch (e) {
      const msg = (e && e.message) ? e.message : String(e);
      return {
        status: 502,
        content: '',
        raw: msg.slice(0, 2000),
      };
    }
  },
);
