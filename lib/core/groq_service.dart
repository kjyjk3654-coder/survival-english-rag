import 'dart:convert';

import 'package:http/http.dart' as http;

class VerifyAnswerResult {
  final int contextScore;
  final bool relevant;
  final String reasonKo;
  final String npcNext;
  final String idealAnswer;
  final String idealAnswerKo;
  final String tipKo;

  const VerifyAnswerResult({
    required this.contextScore,
    required this.relevant,
    required this.reasonKo,
    required this.npcNext,
    required this.idealAnswer,
    required this.idealAnswerKo,
    required this.tipKo,
  });
}

class GroqService {
  final String geminiApiKey;

  const GroqService({required this.geminiApiKey});

  Future<VerifyAnswerResult> verifyAnswer({
    required String themeLabel,
    required String levelLabel,
    required int passThreshold,
    required String npcRole,
    required String situation,
    required List<Map<String, String>> history,
    required String npcLine,
    required String userAnswer,
    String? previousIdealAnswer,
  }) async {
    if (userAnswer.trim().isEmpty) {
      return const VerifyAnswerResult(
        contextScore: 0,
        relevant: false,
        reasonKo: '아무 말도 하지 않았어요.',
        npcNext: 'Sorry—can you say that again?',
        idealAnswer: '(No answer)',
        idealAnswerKo: '(무응답)',
        tipKo: '아주 짧게라도 단어 하나를 말해보세요.',
      );
    }

    final prompt = _judgeAndNextPrompt(
      themeLabel: themeLabel,
      levelLabel: levelLabel,
      passThreshold: passThreshold,
      npcRole: npcRole,
      situation: situation,
      history: history,
      npcLine: npcLine,
      userAnswer: userAnswer,
      previousIdealAnswer: previousIdealAnswer,
    );

    final apiVersions = <String>['v1beta', 'v1'];
    String? usedApi;
    String? usedModel;

    for (final api in apiVersions) {
      final listUri = Uri.parse(
        'https://generativelanguage.googleapis.com/$api/models?key=$geminiApiKey',
      );
      final listRes = await http
          .get(listUri)
          .timeout(const Duration(seconds: 25));
      if (listRes.statusCode != 200) continue;
      final listData = jsonDecode(listRes.body) as Map<String, dynamic>;
      final models = (listData['models'] as List?) ?? const [];
      final candidates = <String>[];
      for (final m in models) {
        final mm = m as Map<String, dynamic>;
        final name = (mm['name'] ?? '').toString();
        final methods =
            (mm['supportedGenerationMethods'] as List?)
                ?.map((e) => e.toString())
                .toList() ??
            const <String>[];
        if (!methods.contains('generateContent')) continue;
        if (!name.startsWith('models/')) continue;
        candidates.add(name.substring('models/'.length));
      }
      String? pick;
      pick = candidates
          .where((e) => e.contains('gemini') && e.contains('flash'))
          .cast<String?>()
          .firstWhere((_) => true, orElse: () => null);
      pick ??= candidates
          .where((e) => e.contains('gemini'))
          .cast<String?>()
          .firstWhere((_) => true, orElse: () => null);
      pick ??= candidates.isNotEmpty ? candidates.first : null;
      if (pick != null && pick.trim().isNotEmpty) {
        usedApi = api;
        usedModel = pick;
        break;
      }
    }

    if (usedApi == null || usedModel == null) {
      throw StateError('No available Gemini model with generateContent');
    }

    final genUri = Uri.parse(
      'https://generativelanguage.googleapis.com/$usedApi/models/$usedModel:generateContent?key=$geminiApiKey',
    );

    Future<String> callOnce({
      required String promptText,
      required double temp,
    }) async {
      final payload = {
        'contents': [
          {
            'role': 'user',
            'parts': [
              {'text': promptText},
            ],
          },
        ],
        'generationConfig': {
          'temperature': temp,
          'maxOutputTokens': 1024,
          'responseMimeType': 'application/json',
        },
      };

      final res = await http
          .post(
            genUri,
            headers: {'Content-Type': 'application/json'},
            body: jsonEncode(payload),
          )
          .timeout(const Duration(seconds: 25));

      if (res.statusCode != 200) {
        final clipped = res.body.length > 900
            ? res.body.substring(0, 900)
            : res.body;
        throw StateError('Gemini http=${res.statusCode} body=$clipped');
      }

      final data = jsonDecode(res.body) as Map<String, dynamic>;
      final candidates = (data['candidates'] as List?) ?? const [];
      final text = (candidates.isNotEmpty)
          ? (((candidates.first as Map)['content'] as Map?)?['parts'] as List?)
                    ?.map((p) => (p as Map)['text']?.toString() ?? '')
                    .join('')
                    .trim() ??
                ''
          : '';
      return text;
    }

    VerifyAnswerResult? parseTurn(String text) {
      if (text.trim().isEmpty) return null;
      final parsed = _parseJsonFromModelText(text);
      if (parsed == null) return null;

      final score = int.tryParse('${parsed['context_score']}') ?? 0;
      final relevant =
          (parsed['relevant'] == true) ||
          (parsed['relevant']?.toString().toLowerCase() == 'true');
      final reasonKo = (parsed['reason_ko'] ?? '').toString().trim();
      final npcNext = (parsed['npc_next'] ?? '').toString().trim();
      final ideal = (parsed['ideal_answer'] ?? '').toString().trim();
      final idealKo = (parsed['ideal_answer_ko'] ?? '').toString().trim();
      final tip = (parsed['tip_ko'] ?? '').toString().trim();

      // Critical invariant: UI must never show blank npc_next / ideal answers.
      // If any is empty, we treat this as a parse failure so that verifyAnswer
      // triggers exactly one Repair request.
      if (npcNext.isEmpty || ideal.isEmpty || idealKo.isEmpty) return null;
      if (reasonKo.isEmpty) return null;

      return VerifyAnswerResult(
        contextScore: score.clamp(0, 100),
        relevant: relevant,
        reasonKo: reasonKo,
        npcNext: npcNext,
        idealAnswer: ideal,
        idealAnswerKo: idealKo,
        tipKo: tip,
      );
    }

    final firstTemp = userAnswer.trim().isEmpty ? 0.0 : 0.7;
    final firstText = await callOnce(promptText: prompt, temp: firstTemp);
    final firstParsed = parseTurn(firstText);
    if (firstParsed != null) {
      if (previousIdealAnswer != null &&
          previousIdealAnswer.trim().isNotEmpty &&
          firstParsed.idealAnswer.trim() == previousIdealAnswer.trim()) {
        // One more repair: keep everything but force ideal_answer to differ.
      } else {
        return firstParsed;
      }
    }

    final repairPrompt =
        """Your previous output was invalid JSON or missing required fields.
Return ONLY one strict JSON object that matches this schema:
{
  "context_score": 0-100,
  "relevant": true/false,
  "reason_ko": "...",
  "npc_next": "...",
  "ideal_answer": "...",
  "ideal_answer_ko": "...",
  "tip_ko": "..."
}
The following fields MUST be non-empty strings: npc_next, ideal_answer, ideal_answer_ko.
Do not include any other text.

Previous output:
$firstText
""";

    final repairedText = await callOnce(promptText: repairPrompt, temp: 0.2);
    final repaired = parseTurn(repairedText);
    if (repaired != null) {
      if (previousIdealAnswer != null &&
          previousIdealAnswer.trim().isNotEmpty &&
          repaired.idealAnswer.trim() == previousIdealAnswer.trim()) {
        final forceDifferentPrompt =
            """Your JSON is valid but ideal_answer repeated the previous turn.
Return ONLY strict JSON with the SAME schema.
Change ideal_answer to a different natural answer from the previous.
Previous ideal_answer: ${previousIdealAnswer.trim()}

Current JSON:
${jsonEncode({'context_score': repaired.contextScore, 'relevant': repaired.relevant, 'reason_ko': repaired.reasonKo, 'npc_next': repaired.npcNext, 'ideal_answer': repaired.idealAnswer, 'ideal_answer_ko': repaired.idealAnswerKo, 'tip_ko': repaired.tipKo})}
""";
        final diffText = await callOnce(
          promptText: forceDifferentPrompt,
          temp: 0.2,
        );
        final diffParsed = parseTurn(diffText);
        if (diffParsed != null &&
            diffParsed.idealAnswer.trim() != previousIdealAnswer.trim()) {
          return diffParsed;
        }
      }
      return repaired;
    }

    if (firstParsed != null) {
      // firstParsed was valid but failed the ideal-change requirement and repairs didn't fix it.
      return firstParsed;
    }

    throw StateError('Could not parse JSON from Gemini output');
  }
}

String _judgeAndNextPrompt({
  required String themeLabel,
  required String levelLabel,
  required int passThreshold,
  required String npcRole,
  required String situation,
  required List<Map<String, String>> history,
  required String npcLine,
  required String userAnswer,
  String? previousIdealAnswer,
}) {
  final prevIdealBlock =
      (previousIdealAnswer != null && previousIdealAnswer.trim().isNotEmpty)
      ? 'Previous turn ideal_answer (MUST be different this turn): ${previousIdealAnswer.trim()}'
      : '';
  return """
You are the sole judge and dialogue engine for a 3-second survival English game.

Theme: $themeLabel
Level: $levelLabel
NPC role: $npcRole
Situation: $situation

Conversation so far (most recent last):
${history.map((e) => "${e['role']}: ${e['text']}").join("\n")}

NPC line (current): $npcLine
User answer (ASR transcription): $userAnswer

$prevIdealBlock

Core policy (SURVIVALISM):
- If the meaning/intention is understandable in context, survival should be granted even with broken grammar or awkward phrasing.
- Only treat as failure when intent is clearly unrelated OR meaning is severely distorted by misunderstanding.
- IMPORTANT: Do NOT auto-pass empty, silence, or gibberish.
- If user answer is empty/meaningless (e.g., only "...", random letters, or unrelated filler), set relevant=false.

Beginner autopass policy (for reliability):
- If userAnswer is NON-EMPTY and contains at least one meaningful English token (e.g., "coffee"), you MUST set relevant=true and context_score >= $passThreshold.
- If userAnswer is EMPTY ("" after trimming), you MUST set relevant=false (boolean false) AND context_score=0. This is non-negotiable.
- If userAnswer is EMPTY, npc_next MUST be a short request to repeat (e.g., "Sorry—can you say that again?").
- You will be tested on this: returning relevant=true for empty input is considered a failure.

Scoring rule:
- Return context_score 0-100.
- Set relevant=true ONLY if context_score >= $passThreshold.

Task:
1) Decide survival (relevant).
2) Generate the next NPC line as a natural tail question or response that continues the conversation.
3) Provide coaching set (ideal answer + Korean meaning + one-line Korean tip).
4) Provide ONE-LINE Korean reason.

Output MUST be strict JSON:
{
  "context_score": 0-100,
  "relevant": true/false,
  "reason_ko": "...",
  "npc_next": "...",
  "ideal_answer": "...",
  "ideal_answer_ko": "...",
  "tip_ko": "..."
}
Return ONLY JSON.
""";
}

Map<String, dynamic>? _parseJsonFromModelText(String raw) {
  final cleaned = _stripCodeFences(raw);
  try {
    return jsonDecode(cleaned) as Map<String, dynamic>;
  } catch (_) {
    final extracted = _extractFirstJsonObject(cleaned);
    if (extracted == null) return null;
    try {
      return jsonDecode(extracted) as Map<String, dynamic>;
    } catch (_) {
      return null;
    }
  }
}

String _stripCodeFences(String s) {
  final t = s.trim();
  if (!t.startsWith('```')) return t;
  final lines = t.split('\n');
  if (lines.isEmpty) return t;
  if (lines.first.trim().startsWith('```')) {
    lines.removeAt(0);
  }
  if (lines.isNotEmpty && lines.last.trim() == '```') {
    lines.removeLast();
  }
  return lines.join('\n').trim();
}

String? _extractFirstJsonObject(String text) {
  final s = text;
  final start = s.indexOf('{');
  if (start < 0) return null;
  int depth = 0;
  bool inString = false;
  for (int i = start; i < s.length; i++) {
    final ch = s[i];
    if (ch == '"') {
      final escaped = (i > 0 && s[i - 1] == '\\');
      if (!escaped) inString = !inString;
    }
    if (inString) continue;
    if (ch == '{') depth++;
    if (ch == '}') {
      depth--;
      if (depth == 0) return s.substring(start, i + 1);
    }
  }
  return null;
}
