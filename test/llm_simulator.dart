import 'dart:io';

import 'package:survival_english/core/game_provider.dart';
import 'package:survival_english/core/groq_service.dart';

class _ThemeSim {
  final String id;
  final String label;
  final String emoji;
  const _ThemeSim(this.id, this.label, this.emoji);
}

class _TurnRow {
  final String themeId;
  final int turn;
  final bool relevant;
  final int score;
  final int gained;
  final String npcNext;
  final String ideal;
  const _TurnRow({
    required this.themeId,
    required this.turn,
    required this.relevant,
    required this.score,
    required this.gained,
    required this.npcNext,
    required this.ideal,
  });
}

Never _fail(String msg) {
  throw StateError(msg);
}

void _assert(bool cond, String msg) {
  if (!cond) _fail(msg);
}

String _readMainDart(String projectRoot) {
  return File('$projectRoot/lib/main.dart').readAsStringSync();
}

String _readConstStringFromMain(String mainDart, String name) {
  final m = RegExp(
    'const\\s+String\\s+$name\\s*=\\s*"([^"]*)";',
  ).firstMatch(mainDart);
  return (m?.group(1) ?? '').trim();
}

List<_ThemeSim> _default10Themes() {
  return const [
    _ThemeSim('cafe', '카페', '☕️'),
    _ThemeSim('airport', '공항', '🛫'),
    _ThemeSim('hotel', '호텔', '🏨'),
    _ThemeSim('clothing', '옷가게', '🛍️'),
    _ThemeSim('workplace', '사회생활', '💼'),
    _ThemeSim('neighborhood', '이웃', '🏘️'),
    _ThemeSim('directions', '길찾기', '📍'),
    _ThemeSim('interview', '면접', '🎤'),
    _ThemeSim('party', '파티', '🥳'),
    _ThemeSim('hospital', '병원', '🏥'),
  ];
}

void _assertScoringAndGameOver() {
  final gp = GameProvider()..resetRun();
  gp.startNewTurn();
  _assert(
    gp.gainedPointsForCurrentTurn() == 10,
    '버그 재발: 점수 수식 오류 (revive=0 expected +10 got ${gp.gainedPointsForCurrentTurn()})',
  );
  gp.tryUseRevive();
  _assert(
    gp.gainedPointsForCurrentTurn() == 9,
    '버그 재발: 점수 수식 오류 (revive=1 expected +9 got ${gp.gainedPointsForCurrentTurn()})',
  );
  gp.tryUseRevive();
  _assert(
    gp.gainedPointsForCurrentTurn() == 8,
    '버그 재발: 점수 수식 오류 (revive=2 expected +8 got ${gp.gainedPointsForCurrentTurn()})',
  );
  gp.tryUseRevive();
  _assert(
    gp.gainedPointsForCurrentTurn() == 7,
    '버그 재발: 점수 수식 오류 (revive=3 expected +7 got ${gp.gainedPointsForCurrentTurn()})',
  );

  final gp2 = GameProvider()..resetRun();
  _assert(!gp2.isGameOver, '버그: 초기 상태에서 gameOver=true');
  gp2.registerTurnFailure();
  gp2.registerTurnFailure();
  gp2.registerTurnFailure();
  _assert(!gp2.isGameOver, '버그: 3회 실패인데 gameOver=true');
  gp2.registerTurnFailure();
  _assert(gp2.isGameOver, '버그 재발: 4회 실패인데 gameOver=false');
}

Future<void> _assertVerifyAnswerCases({
  required GroqService groq,
  required _ThemeSim theme,
  required List<_TurnRow> rowsOut,
}) async {
  final gp = GameProvider()..resetRun();
  const passThreshold = 70;
  const npcRole = 'clerk';
  final situation = '${theme.emoji} ${theme.label} 상황에서 대화 중';

  final history = <Map<String, String>>[];
  String npcLine = 'What would you like?';
  String prevIdealAnswer = '';

  for (int turn = 1; turn <= 10; turn++) {
    gp.startNewTurn();

    final userAnswer = (turn == 3) ? '' : 'Coffee';

    final result = await groq.verifyAnswer(
      themeLabel: theme.label,
      levelLabel: 'Beginner',
      passThreshold: passThreshold,
      npcRole: npcRole,
      situation: situation,
      history: history,
      npcLine: npcLine,
      userAnswer: userAnswer,
      previousIdealAnswer: prevIdealAnswer,
    );

    if (userAnswer.isEmpty) {
      _assert(
        result.relevant == false,
        '버그 재발: ${theme.id} 테마에서 침묵("")을 relevant=true로 통과시킴',
      );
      gp.registerTurnFailure();
      rowsOut.add(
        _TurnRow(
          themeId: theme.id,
          turn: turn,
          relevant: result.relevant,
          score: gp.score,
          gained: 0,
          npcNext: result.npcNext,
          ideal: result.idealAnswer,
        ),
      );
      // 실패 턴은 npcLine/his 업데이트 안 함 (앱과 동일하게 재시도 개념)
      continue;
    }

    _assert(
      result.relevant == true,
      '버그 재발: ${theme.id} 테마에서 단답("Coffee")을 relevant=false로 실패 처리',
    );

    String? prevIdeal;
    if (rowsOut.isNotEmpty) {
      final prev = rowsOut.lastWhere(
        (r) => r.themeId == theme.id && r.relevant,
        orElse: () => const _TurnRow(
          themeId: '',
          turn: 0,
          relevant: false,
          score: 0,
          gained: 0,
          npcNext: '',
          ideal: '',
        ),
      );
      if (prev.themeId == theme.id && prev.relevant) {
        prevIdeal = prev.ideal;
      }
    }
    if (prevIdeal != null && prevIdeal.trim().isNotEmpty) {
      _assert(
        result.idealAnswer.trim() != prevIdeal.trim(),
        '버그 재발: ${theme.id} 테마에서 모범답안 고정됨 (prev="$prevIdeal" now="${result.idealAnswer}")',
      );
    }

    final gained = gp.gainedPointsForCurrentTurn();
    gp.registerTurnSuccess();

    rowsOut.add(
      _TurnRow(
        themeId: theme.id,
        turn: turn,
        relevant: result.relevant,
        score: gp.score,
        gained: gained,
        npcNext: result.npcNext,
        ideal: result.idealAnswer,
      ),
    );

    prevIdealAnswer = result.idealAnswer;

    history.add({'role': 'npc', 'text': npcLine});
    history.add({'role': 'user', 'text': userAnswer});
    npcLine = result.npcNext;
  }
}

void _printTable(List<_TurnRow> rows) {
  stdout.writeln('');
  stdout.writeln(
    '| theme | turn | relevant | gained | score | ideal_answer | npc_next |',
  );
  stdout.writeln('|---|---:|:---:|---:|---:|---|---|');
  for (final r in rows) {
    final ideal = r.ideal.replaceAll('\n', ' ');
    final npc = r.npcNext.replaceAll('\n', ' ');
    final idealClip = ideal.length > 48 ? '${ideal.substring(0, 48)}…' : ideal;
    final npcClip = npc.length > 48 ? '${npc.substring(0, 48)}…' : npc;
    stdout.writeln(
      '| ${r.themeId} | ${r.turn} | ${r.relevant ? 'T' : 'F'} | ${r.gained} | ${r.score} | $idealClip | $npcClip |',
    );
  }
}

Future<void> main(List<String> args) async {
  final projectRoot = args.isNotEmpty ? args.first : Directory.current.path;
  final mainDart = _readMainDart(projectRoot);
  final geminiApiKey = _readConstStringFromMain(mainDart, 'geminiApiKey');

  _assert(
    geminiApiKey.isNotEmpty,
    'FAIL: geminiApiKey not found in lib/main.dart',
  );

  _assertScoringAndGameOver();

  final groq = GroqService(geminiApiKey: geminiApiKey);
  final themes = _default10Themes();
  final rows = <_TurnRow>[];

  for (final t in themes) {
    await _assertVerifyAnswerCases(groq: groq, theme: t, rowsOut: rows);
  }

  _printTable(rows);
  stdout.writeln('');
  stdout.writeln('[ALL BUGS CLEARED - 자체 검증 100% 통과]');
}
