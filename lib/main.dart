import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'package:flutter/foundation.dart' show kDebugMode, kIsWeb;
import 'package:flutter/material.dart';
import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:firebase_core/firebase_core.dart';
import 'package:flutter_tts/flutter_tts.dart';
import 'package:record/record.dart';
import 'package:http/http.dart' as http;
import 'package:speech_to_text/speech_to_text.dart' as stt;
import 'package:audioplayers/audioplayers.dart';
import 'package:path_provider/path_provider.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:google_mobile_ads/google_mobile_ads.dart';
import 'package:firebase_analytics/firebase_analytics.dart';
import 'package:survival_english/core/game_provider.dart';
import 'package:firebase_auth/firebase_auth.dart';
import 'package:google_sign_in/google_sign_in.dart';
import 'firebase_options.dart';

const String _debugBuildTag = 'debug-20260401-1318';

final FirebaseAuth _auth = FirebaseAuth.instance;
final GoogleSignIn _googleSignIn = GoogleSignIn();

Future<User?> _signInWithGoogle() async {
  final account = await _googleSignIn.signIn();
  if (account == null) return null;
  final auth = await account.authentication;
  if (auth.accessToken == null || auth.idToken == null) return null;
  final credential = GoogleAuthProvider.credential(
    accessToken: auth.accessToken,
    idToken: auth.idToken,
  );
  final result = await _auth.signInWithCredential(credential);
  return result.user;
}

Future<void> _signOutGoogle() async {
  try {
    await _googleSignIn.signOut();
  } catch (_) {
    // ignore
  }
  await _auth.signOut();
}

Future<String?> _currentAuthUidOrNull() async {
  return _auth.currentUser?.uid;
}

String _defaultNicknameFromDisplayName(String? displayName) {
  final base = (displayName ?? '').trim();
  final cleaned = base.replaceAll(RegExp(r'[^A-Za-z0-9]'), '');
  if (cleaned.length >= 3) {
    return cleaned.length > 16 ? cleaned.substring(0, 16) : cleaned;
  }
  return _generateHipNickname();
}

Future<void> _ensureUserProfileInitialized(User user) async {
  final userRef = FirebaseFirestore.instance.collection('users').doc(user.uid);
  final snap = await userRef.get();
  if (snap.exists) return;
  final nickname = _defaultNicknameFromDisplayName(user.displayName);
  await userRef.set({
    'nickname': nickname,
    'display_name': user.displayName ?? '',
    'created_at': FieldValue.serverTimestamp(),
    'updated_at': FieldValue.serverTimestamp(),
  }, SetOptions(merge: true));
}

Future<String> _getOrCreateFirestoreNickname(String uid) async {
  final userRef = FirebaseFirestore.instance.collection('users').doc(uid);
  final snap = await userRef.get();
  final existing = (snap.data()?['nickname'] ?? '').toString().trim();
  if (existing.isNotEmpty && _isValidNickname(existing)) return existing;
  final nickname = _defaultNicknameFromDisplayName(
    _auth.currentUser?.displayName,
  );
  await userRef.set({
    'nickname': nickname,
    'display_name': _auth.currentUser?.displayName ?? '',
    'updated_at': FieldValue.serverTimestamp(),
  }, SetOptions(merge: true));
  return nickname;
}

Future<void> _setFirestoreNickname(String uid, String nickname) async {
  final n = nickname.trim();
  if (n.isEmpty) return;
  await FirebaseFirestore.instance.collection('users').doc(uid).set({
    'nickname': n,
    'updated_at': FieldValue.serverTimestamp(),
  }, SetOptions(merge: true));
}

List<String> _weekKeysForMigration() {
  final keys = <String>[];
  final now = DateTime.now();
  for (int i = 0; i < 12; i++) {
    keys.add(_yearWeekKey(now.subtract(Duration(days: i * 7))));
  }
  return keys.toSet().toList();
}

Future<void> _migrateLegacyLeaderboardScoresToAuthUid({
  required String newUid,
}) async {
  const legacyKey = 'device_user_id_v1';
  final prefs = await SharedPreferences.getInstance();
  final oldUid = (prefs.getString(legacyKey) ?? '').trim();
  if (oldUid.isEmpty || oldUid == newUid) return;

  final nickname = await _getOrCreateFirestoreNickname(newUid);

  final weeks = _weekKeysForMigration();
  for (final weekKey in weeks) {
    for (final level in SurvivalLevel.values) {
      final docId = '${weekKey}_${levelId(level)}';
      final base = FirebaseFirestore.instance
          .collection('weekly_leaderboards')
          .doc(docId)
          .collection('scores');
      final oldRef = base.doc(oldUid);
      final newRef = base.doc(newUid);
      await FirebaseFirestore.instance.runTransaction((tx) async {
        final oldSnap = await tx.get(oldRef);
        if (!oldSnap.exists) return;
        final oldScore = (oldSnap.data()?['score'] as num?)?.toInt() ?? 0;
        if (oldScore <= 0) return;
        final newSnap = await tx.get(newRef);
        final newScore = (newSnap.data()?['score'] as num?)?.toInt() ?? 0;
        if (oldScore <= newScore) return;
        tx.set(newRef, {
          'score': oldScore,
          'nickname': nickname,
          'theme_id': (oldSnap.data()?['theme_id'] ?? '').toString(),
          'level': levelId(level),
          'week_key': weekKey,
          'migrated_from': oldUid,
          'build_tag': _debugBuildTag,
          'platform': kIsWeb ? 'web' : Platform.operatingSystem,
          'updated_at': FieldValue.serverTimestamp(),
        }, SetOptions(merge: true));
      });
    }
  }

  await prefs.setString('legacy_migrated_to_uid_v1', newUid);
}

const String groqApiKey = String.fromEnvironment(
  'GROQ_API_KEY',
  defaultValue: '',
);

const String geminiApiKey = String.fromEnvironment(
  'GEMINI_API_KEY',
  defaultValue: '',
);

const String openAiApiKey = String.fromEnvironment(
  'OPENAI_API_KEY',
  defaultValue: '',
);

final FirebaseAnalytics analytics = FirebaseAnalytics.instance;

void logEventSafe(String name, {Map<String, Object?>? parameters}) {
  try {
    final Map<String, Object>? safeParams = parameters == null
        ? null
        : <String, Object>{
            for (final e in parameters.entries)
              if (e.value != null) e.key: e.value as Object,
          };
    analytics.logEvent(name: name, parameters: safeParams);
  } catch (e) {
    debugPrint('[Analytics] logEvent failed name=$name err=$e');
  }
}

int _dayOfYear(DateTime d) {
  final start = DateTime(d.year, 1, 1);
  return d.difference(start).inDays + 1;
}

String _yearWeekKey(DateTime now) {
  final d = DateTime(now.year, now.month, now.day);
  final dayOfYear = _dayOfYear(d);
  final w = ((dayOfYear - d.weekday + 10) / 7).floor();
  final week = w < 1 ? 1 : w;
  return '${d.year}-W${week.toString().padLeft(2, '0')}';
}

String _generateHipNickname() {
  const adjectives = [
    'Brave',
    'Chill',
    'Bold',
    'Swift',
    'Witty',
    'Calm',
    'Lucky',
    'Sunny',
    'Neon',
    'Quiet',
    'Fierce',
    'Hyper',
    'Gentle',
    'Nimble',
    'Cosmic',
  ];
  const nouns = [
    'Panda',
    'Tiger',
    'Falcon',
    'Otter',
    'Dolphin',
    'Fox',
    'Koala',
    'Raven',
    'Shark',
    'Dragon',
    'Robot',
    'Wizard',
    'Ninja',
    'Pilot',
    'Skater',
  ];
  final a = adjectives[Random().nextInt(adjectives.length)];
  final n = nouns[Random().nextInt(nouns.length)];
  final num = (100 + Random().nextInt(900)).toString();
  return '$a$n$num';
}

final RegExp _nicknameAllowed = RegExp(r'^[A-Za-z0-9]{3,16}$');

String _normalizeForProfanityCheck(String s) {
  return s
      .toLowerCase()
      .replaceAll('0', 'o')
      .replaceAll('1', 'i')
      .replaceAll('3', 'e')
      .replaceAll('4', 'a')
      .replaceAll('5', 's')
      .replaceAll('7', 't')
      .replaceAll('@', 'a')
      .replaceAll('', '')
      .replaceAll(RegExp(r'[^a-z]'), '');
}

bool _containsProfanity(String s) {
  final t = _normalizeForProfanityCheck(s);
  if (t.isEmpty) return false;
  const banned = <String>[
    'fuck',
    'fuk',
    'shit',
    'bitch',
    'asshole',
    'bastard',
    'cunt',
    'dick',
    'pussy',
    'slut',
    'whore',
    'nigger',
    'faggot',
    'rape',
    'suicide',
    'kill',
    'sex',
    'porno',
    'porn',
    'hentai',
    'nazi',
  ];
  for (final w in banned) {
    if (t.contains(w)) return true;
  }
  return false;
}

bool _isValidNickname(String nickname) {
  final n = nickname.trim();
  if (!_nicknameAllowed.hasMatch(n)) return false;
  if (_containsProfanity(n)) return false;
  return true;
}

Future<String> _getOrCreateNickname() async {
  final uid = await _currentAuthUidOrNull();
  if (uid != null) {
    return _getOrCreateFirestoreNickname(uid);
  }
  const k = 'device_nickname_v1';
  final prefs = await SharedPreferences.getInstance();
  final existing = (prefs.getString(k) ?? '').trim();
  if (existing.isNotEmpty && _isValidNickname(existing)) return existing;
  String nick = _generateHipNickname();
  for (int i = 0; i < 12; i++) {
    if (_isValidNickname(nick)) break;
    nick = _generateHipNickname();
  }
  if (!_isValidNickname(nick)) {
    nick = 'Player${100 + Random().nextInt(900)}';
  }
  await prefs.setString(k, nick);
  return nick;
}

Future<void> _setNickname(String nickname) async {
  final uid = await _currentAuthUidOrNull();
  if (uid != null) {
    await _setFirestoreNickname(uid, nickname);
    return;
  }
  const k = 'device_nickname_v1';
  final n = nickname.trim();
  if (n.isEmpty) return;
  final prefs = await SharedPreferences.getInstance();
  await prefs.setString(k, n);
}

Future<void> _uploadWeeklyBestScore({
  required SurvivalLevel level,
  required int score,
  required String themeId,
}) async {
  if (score <= 0) return;
  final uid = await _currentAuthUidOrNull();
  if (uid == null) return;
  final nickname = await _getOrCreateNickname();
  final weekKey = _yearWeekKey(DateTime.now());
  final docId = '${weekKey}_${levelId(level)}';

  final userRef = FirebaseFirestore.instance
      .collection('weekly_leaderboards')
      .doc(docId)
      .collection('scores')
      .doc(uid);

  await FirebaseFirestore.instance.runTransaction((tx) async {
    final snap = await tx.get(userRef);
    final prev = (snap.data()?['score'] as num?)?.toInt() ?? 0;
    if (score <= prev) return;
    tx.set(userRef, {
      'score': score,
      'nickname': nickname,
      'theme_id': themeId,
      'level': levelId(level),
      'week_key': weekKey,
      'build_tag': _debugBuildTag,
      'platform': kIsWeb ? 'web' : Platform.operatingSystem,
      'updated_at': FieldValue.serverTimestamp(),
    }, SetOptions(merge: true));
  });
}

class LeaderboardEntry {
  final String userId;
  final String nickname;
  final int score;
  final DateTime updatedAt;
  const LeaderboardEntry({
    required this.userId,
    required this.nickname,
    required this.score,
    required this.updatedAt,
  });
}

class LeaderboardPage extends StatefulWidget {
  const LeaderboardPage({super.key});

  @override
  State<LeaderboardPage> createState() => _LeaderboardPageState();
}

class _LeaderboardPageState extends State<LeaderboardPage>
    with SingleTickerProviderStateMixin {
  late final TabController _tab;
  late Future<String> _nickFuture;
  bool _signingIn = false;

  @override
  void initState() {
    super.initState();
    _tab = TabController(length: SurvivalLevel.values.length, vsync: this);
    _nickFuture = _getOrCreateNickname();
  }

  @override
  void dispose() {
    _tab.dispose();
    super.dispose();
  }

  Future<List<LeaderboardEntry>> _fetchTop({
    required String weekKey,
    required SurvivalLevel level,
  }) async {
    final docId = '${weekKey}_${levelId(level)}';
    final snap = await FirebaseFirestore.instance
        .collection('weekly_leaderboards')
        .doc(docId)
        .collection('scores')
        .orderBy('score', descending: true)
        .limit(50)
        .get();
    return snap.docs.map((d) {
      final data = d.data();
      final score = (data['score'] as num?)?.toInt() ?? 0;
      final nickname = (data['nickname'] ?? '').toString().trim();
      final ts = data['updated_at'];
      final updated = ts is Timestamp ? ts.toDate() : DateTime.now();
      return LeaderboardEntry(
        userId: d.id,
        nickname: nickname,
        score: score,
        updatedAt: updated,
      );
    }).toList();
  }

  Future<void> _editNickname() async {
    final current = await _getOrCreateNickname();
    if (!mounted) return;
    final controller = TextEditingController(text: current);
    final ok = await showDialog<bool>(
      context: context,
      builder: (ctx) {
        return AlertDialog(
          title: const Text('닉네임 변경'),
          content: TextField(
            controller: controller,
            decoration: const InputDecoration(hintText: '예: BravePanda247'),
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(ctx).pop(false),
              child: const Text('취소'),
            ),
            TextButton(
              onPressed: () => Navigator.of(ctx).pop(true),
              child: const Text('저장'),
            ),
          ],
        );
      },
    );
    if (ok != true) return;
    final next = controller.text.trim();
    if (next.isEmpty) return;
    if (!_isValidNickname(next)) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text('사용할 수 없는 닉네임입니다. (영문/숫자 3~16자, 욕설/금칙어 불가)'),
        ),
      );
      return;
    }
    await _setNickname(next);
    if (!mounted) return;
    setState(() {
      _nickFuture = Future.value(next);
    });
  }

  Future<void> _handleGoogleSignIn() async {
    if (_signingIn) return;
    setState(() {
      _signingIn = true;
    });
    try {
      final user = await _signInWithGoogle();
      if (user == null) return;
      await _ensureUserProfileInitialized(user);
      await _migrateLegacyLeaderboardScoresToAuthUid(newUid: user.uid);
      if (!mounted) return;
      setState(() {
        _nickFuture = _getOrCreateNickname();
      });
    } catch (e) {
      debugPrint('[auth] google_signin_failed err=$e');
      if (!mounted) return;
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text('구글 로그인 실패: $e')));
    } finally {
      if (mounted) {
        setState(() {
          _signingIn = false;
        });
      }
    }
  }

  Future<void> _handleSignOut() async {
    await _signOutGoogle();
    if (!mounted) return;
    setState(() {
      _nickFuture = _getOrCreateNickname();
    });
  }

  @override
  Widget build(BuildContext context) {
    final weekKey = _yearWeekKey(DateTime.now());
    return StreamBuilder<User?>(
      stream: _auth.authStateChanges(),
      builder: (context, snapUser) {
        final user = snapUser.data;
        return Scaffold(
          backgroundColor: const Color(0xFF0A0A0A),
          appBar: AppBar(
            title: const Text('주간 랭킹'),
            backgroundColor: Colors.transparent,
            actions: [
              if (user != null)
                IconButton(
                  onPressed: _editNickname,
                  icon: const Icon(Icons.edit_outlined),
                ),
              if (user != null)
                IconButton(
                  onPressed: _handleSignOut,
                  icon: const Icon(Icons.logout_outlined),
                ),
            ],
            bottom: TabBar(
              controller: _tab,
              tabs: [
                for (final l in SurvivalLevel.values) Tab(text: levelLabel(l)),
              ],
            ),
          ),
          body: user == null
              ? Center(
                  child: Padding(
                    padding: const EdgeInsets.all(24),
                    child: Column(
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        const Text(
                          '랭킹에 기록을 남기려면\n구글 로그인이 필요해요.',
                          textAlign: TextAlign.center,
                        ),
                        const SizedBox(height: 16),
                        ElevatedButton.icon(
                          onPressed: _signingIn ? null : _handleGoogleSignIn,
                          icon: const Icon(Icons.login_outlined),
                          label: Text(_signingIn ? '로그인 중...' : 'Google로 로그인'),
                        ),
                      ],
                    ),
                  ),
                )
              : FutureBuilder<String>(
                  future: _nickFuture,
                  builder: (context, snapNick) {
                    final myNick = snapNick.data;
                    final uid = user.uid;
                    return TabBarView(
                      controller: _tab,
                      children: [
                        for (final l in SurvivalLevel.values)
                          FutureBuilder<List<LeaderboardEntry>>(
                            future: _fetchTop(weekKey: weekKey, level: l),
                            builder: (context, snap) {
                              final list =
                                  snap.data ?? const <LeaderboardEntry>[];
                              if (snap.connectionState !=
                                  ConnectionState.done) {
                                return const Center(
                                  child: CircularProgressIndicator(
                                    color: Colors.cyanAccent,
                                  ),
                                );
                              }
                              if (list.isEmpty) {
                                return const Center(child: Text('아직 기록이 없어요.'));
                              }
                              return ListView.separated(
                                padding: const EdgeInsets.all(16),
                                itemCount: list.length,
                                separatorBuilder: (context, index) =>
                                    const Divider(height: 1),
                                itemBuilder: (context, i) {
                                  final e = list[i];
                                  final me = (e.userId == uid);
                                  final display = (e.nickname.trim().isNotEmpty)
                                      ? e.nickname.trim()
                                      : (me && (myNick ?? '').trim().isNotEmpty)
                                      ? (myNick ?? '').trim()
                                      : e.userId;
                                  return ListTile(
                                    leading: Text('${i + 1}'),
                                    title: Text(me ? '나 · $display' : display),
                                    subtitle: Text(
                                      '업데이트: ${e.updatedAt.toLocal()}',
                                    ),
                                    trailing: Text(
                                      '${e.score}',
                                      style: const TextStyle(
                                        fontWeight: FontWeight.w900,
                                      ),
                                    ),
                                  );
                                },
                              );
                            },
                          ),
                      ],
                    );
                  },
                ),
        );
      },
    );
  }
}

void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await Firebase.initializeApp(options: DefaultFirebaseOptions.currentPlatform);
  await MobileAds.instance.initialize();
  runApp(const SurvivalEnglishApp());
}

class SurvivalEnglishApp extends StatelessWidget {
  const SurvivalEnglishApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        useMaterial3: true,
        colorSchemeSeed: Colors.cyan,
        brightness: Brightness.dark,
      ),
      home: const TitlePage(),
    );
  }
}

enum SurvivalLevel { beginner, intermediate, advanced, hardcore }

String levelId(SurvivalLevel level) {
  switch (level) {
    case SurvivalLevel.beginner:
      return "beginner";
    case SurvivalLevel.intermediate:
      return "intermediate";
    case SurvivalLevel.advanced:
      return "advanced";
    case SurvivalLevel.hardcore:
      return "hardcore";
  }
}

String levelLabel(SurvivalLevel level) {
  switch (level) {
    case SurvivalLevel.beginner:
      return "초급";
    case SurvivalLevel.intermediate:
      return "중급";
    case SurvivalLevel.advanced:
      return "고급";
    case SurvivalLevel.hardcore:
      return "최상급";
  }
}

int maxRoundsForLevel(SurvivalLevel level) {
  switch (level) {
    case SurvivalLevel.beginner:
      return 3;
    case SurvivalLevel.intermediate:
      return 5;
    case SurvivalLevel.advanced:
      return 7;
    case SurvivalLevel.hardcore:
      return 10;
  }
}

class Scenario {
  final String id;
  final String title;
  final String npcRole;
  final String situation;
  final String questionEn;
  final String situationKo;
  final List<String> keywords;
  const Scenario({
    required this.id,
    required this.title,
    required this.npcRole,
    required this.situation,
    this.questionEn = '',
    this.situationKo = '',
    this.keywords = const [],
  });

  Map<String, dynamic> toJson() => {
    'id': id,
    'title': title,
    'npcRole': npcRole,
    'situation': situation,
    'question_en': questionEn,
    'situation_ko': situationKo,
    'keywords': keywords,
  };

  static Scenario fromJson(Map<String, dynamic> json) {
    return Scenario(
      id: (json['id'] ?? '').toString(),
      title: (json['title'] ?? '').toString(),
      npcRole: (json['npcRole'] ?? '').toString(),
      situation: (json['situation'] ?? '').toString(),
      questionEn: (json['question_en'] ?? '').toString(),
      situationKo: (json['situation_ko'] ?? '').toString(),
      keywords: ((json['keywords'] as List?) ?? const [])
          .map((e) => e.toString())
          .toList(),
    );
  }
}

class ThemeDef {
  final String id;
  final String label;
  final String emoji;
  const ThemeDef({required this.id, required this.label, required this.emoji});

  Map<String, dynamic> toJson() => {'id': id, 'label': label, 'emoji': emoji};

  static ThemeDef fromJson(Map<String, dynamic> json) {
    return ThemeDef(
      id: (json['id'] ?? '').toString(),
      label: (json['label'] ?? '').toString(),
      emoji: (json['emoji'] ?? '').toString(),
    );
  }
}

class WeeklyContent {
  final int week;
  final List<ThemeDef> themes;
  final Map<String, Map<String, List<Scenario>>> scenariosByThemeAndLevel;
  const WeeklyContent({
    required this.week,
    required this.themes,
    required this.scenariosByThemeAndLevel,
  });

  Map<String, dynamic> toJson() {
    return {
      'week': week,
      'themes': themes.map((t) => t.toJson()).toList(),
      'scenarios': scenariosByThemeAndLevel.map((themeId, byLevel) {
        return MapEntry(
          themeId,
          byLevel.map(
            (level, list) =>
                MapEntry(level, list.map((s) => s.toJson()).toList()),
          ),
        );
      }),
    };
  }

  static WeeklyContent? fromJson(Map<String, dynamic> json) {
    try {
      final week = int.tryParse((json['week'] ?? '0').toString()) ?? 0;
      final themeList = ((json['themes'] as List?) ?? []).cast<dynamic>();
      final themes = themeList
          .map((e) => ThemeDef.fromJson(Map<String, dynamic>.from(e as Map)))
          .toList();
      final scenariosRaw = Map<String, dynamic>.from(
        (json['scenarios'] as Map?) ?? {},
      );
      final scenarios = <String, Map<String, List<Scenario>>>{};
      for (final entry in scenariosRaw.entries) {
        final themeId = entry.key;
        final byLevelRaw = Map<String, dynamic>.from(entry.value as Map);
        final byLevel = <String, List<Scenario>>{};
        for (final e2 in byLevelRaw.entries) {
          final level = e2.key;
          final listRaw = ((e2.value as List?) ?? []).cast<dynamic>();
          byLevel[level] = listRaw
              .map(
                (e) => Scenario.fromJson(Map<String, dynamic>.from(e as Map)),
              )
              .toList();
        }
        scenarios[themeId] = byLevel;
      }
      return WeeklyContent(
        week: week,
        themes: themes,
        scenariosByThemeAndLevel: scenarios,
      );
    } catch (_) {
      return null;
    }
  }
}

class FirestoreContentStore extends ChangeNotifier {
  static const String _prefsCacheKey = 'weekly_content_cache_v1';
  static const String _prefsWeekKey = 'weekly_content_week_v1';

  WeeklyContent? _content;
  bool _loading = true;
  String? _error;

  WeeklyContent? get content => _content;
  bool get loading => _loading;
  String? get error => _error;

  final AiClient _ai = FailoverAiClient(
    primary: GeminiAiClient(),
    secondary: OpenAiAiClient(),
  );

  static final RegExp _badWordRegex = RegExp(
    r'\b(fuck|shit|bitch|asshole|bastard|cunt|nigger|faggot|slut|whore|dick|pussy|rape|suicide|kill)\b|'
    r'(씨발|시발|ㅅㅂ|병신|좆|새끼|미친년|미친놈|꺼져)',
    caseSensitive: false,
  );

  bool _containsBadWords(String text) {
    final s = text.trim();
    if (s.isEmpty) return false;
    return _badWordRegex.hasMatch(s);
  }

  static const Duration _scenarioCooldownDuration = Duration(seconds: 75);
  final Map<String, Future<int>> _scenarioGenInflight = {};
  final Map<String, DateTime> _scenarioGenCooldownUntil = {};

  String _scenarioGenKey(String themeId, SurvivalLevel level) {
    return '$themeId:${levelId(level)}';
  }

  Future<void> _persistCache() async {
    final c = _content;
    if (c == null) return;
    final prefs = await SharedPreferences.getInstance();
    await prefs.setInt(_prefsWeekKey, c.week);
    await prefs.setString(_prefsCacheKey, jsonEncode(c.toJson()));
  }

  ThemeDef _themeDefOrFallback(String themeId) {
    final themes = _content?.themes ?? const <ThemeDef>[];
    for (final t in themes) {
      if (t.id == themeId) return t;
    }
    return ThemeDef(id: themeId, label: themeId, emoji: '🎭');
  }

  List<String> _recentSituationHints(String themeId, SurvivalLevel level) {
    final byTheme = _content?.scenariosByThemeAndLevel[themeId];
    final list = byTheme?[levelId(level)] ?? const <Scenario>[];
    final out = <String>[];
    for (final s in list.reversed) {
      final hint = s.situation.trim();
      if (hint.isEmpty) continue;
      out.add(hint);
      if (out.length >= 12) break;
    }
    return out;
  }

  String _safeScenarioDocId(String themeId, SurvivalLevel level, int i) {
    final ts = DateTime.now().millisecondsSinceEpoch;
    return '${themeId}_${levelId(level)}_g_${ts}_${i.toString().padLeft(2, '0')}';
  }

  Future<int> ensureScenarioCache({
    required String themeId,
    required SurvivalLevel level,
    int minCount = 10,
  }) async {
    final key = _scenarioGenKey(themeId, level);
    final until = _scenarioGenCooldownUntil[key];
    if (until != null && DateTime.now().isBefore(until)) {
      debugPrint(
        '[Scenario_Cache] cooldown key=$key until=${until.toIso8601String()}',
      );
      return 0;
    }

    final inflight = _scenarioGenInflight[key];
    if (inflight != null) {
      return inflight;
    }

    final fut = _ensureScenarioCacheImpl(
      themeId: themeId,
      level: level,
      minCount: minCount,
      key: key,
    );
    _scenarioGenInflight[key] = fut;
    return fut.whenComplete(() {
      _scenarioGenInflight.remove(key);
    });
  }

  Future<int> _ensureScenarioCacheImpl({
    required String themeId,
    required SurvivalLevel level,
    required int minCount,
    required String key,
  }) async {
    if (geminiApiKey.trim().isEmpty) return 0;
    try {
      final themeRef = FirebaseFirestore.instance
          .collection('themes')
          .doc(themeId);
      int totalWritten = 0;
      const int maxAttempts = 3;
      for (int attempt = 0; attempt < maxAttempts; attempt++) {
        final existingSnap = await themeRef
            .collection('scenarios')
            .where('level', isEqualTo: levelId(level))
            .limit(minCount)
            .get();
        final existingCount = existingSnap.size;
        if (existingCount >= minCount) break;

        final theme = _themeDefOrFallback(themeId);
        final recentHints = _recentSituationHints(themeId, level);
        final need = (minCount - existingCount).clamp(1, 10);
        final batchItems = await _ai.generateScenarioBatch(
          theme: theme,
          level: level,
          count: need,
          recentSituationHints: recentHints,
        );
        if (batchItems == null || batchItems.isEmpty) {
          if (_ai.lastHttpStatus == 429) {
            _scenarioGenCooldownUntil[key] = DateTime.now().add(
              _scenarioCooldownDuration,
            );
          }
          break;
        }

        int written = 0;
        final writeBatch = FirebaseFirestore.instance.batch();
        for (int i = 0; i < batchItems.length; i++) {
          final item = batchItems[i];
          final titleKo = (item['title_ko'] ?? '').toString().trim();
          final npcRole = (item['npcRole'] ?? item['npc_role'] ?? '')
              .toString()
              .trim();
          final situation = (item['situation'] ?? '').toString().trim();
          final situationKo = (item['situation_ko'] ?? '').toString().trim();
          final questionEn = (item['question_en'] ?? '').toString().trim();
          final keywords = ((item['keywords'] as List?) ?? const [])
              .map((e) => e.toString().trim().toLowerCase())
              .where((e) => e.isNotEmpty)
              .toList();

          if (situation.isEmpty || npcRole.isEmpty) continue;

          final combined = [
            titleKo,
            npcRole,
            situation,
            situationKo,
            questionEn,
            keywords.join(' '),
          ].join(' ');
          if (_containsBadWords(combined)) {
            final trashRef = themeRef.collection('trash').doc();
            writeBatch.set(trashRef, {
              'reason': 'bad_word',
              'themeId': themeId,
              'level': levelId(level),
              'generated': true,
              'item': {
                'title_ko': titleKo,
                'npcRole': npcRole,
                'situation': situation,
                'situation_ko': situationKo,
                'question_en': questionEn,
                'keywords': keywords,
              },
              'created_at': DateTime.now().toUtc().toIso8601String(),
            });
            continue;
          }

          final docId = _safeScenarioDocId(themeId, level, (attempt * 20) + i);
          final ref = themeRef.collection('scenarios').doc(docId);
          final nowIso = DateTime.now().toUtc().toIso8601String();
          writeBatch.set(ref, {
            'title': titleKo.isEmpty ? docId : titleKo,
            'title_ko': titleKo.isEmpty ? docId : titleKo,
            'npcRole': npcRole,
            'situation': situation,
            'situation_ko': situationKo,
            'question_en': questionEn,
            'keywords': keywords,
            'level': levelId(level),
            'generated': true,
            'status': 'active',
            'reportCount': 0,
            'lastModifiedBy': 'ai',
            'themeId': themeId,
            'created_at': nowIso,
            'updated_at': nowIso,
          });
          written++;
        }

        if (written <= 0) break;
        await writeBatch.commit();
        totalWritten += written;
      }

      final refreshed = await themeRef
          .collection('scenarios')
          .where('level', isEqualTo: levelId(level))
          .get();
      final list = <Scenario>[];
      for (final d in refreshed.docs) {
        final data = d.data();
        final title = (data['title_ko'] ?? data['title'] ?? d.id).toString();
        final npcRole = (data['npcRole'] ?? data['npc_role'] ?? 'staff')
            .toString();
        final situation = (data['situation'] ?? '').toString();
        final questionEn = (data['question_en'] ?? '').toString();
        final situationKo = (data['situation_ko'] ?? '').toString();
        final keywords = ((data['keywords'] as List?) ?? const [])
            .map((e) => e.toString())
            .toList();
        list.add(
          Scenario(
            id: d.id,
            title: title,
            npcRole: npcRole,
            situation: situation,
            questionEn: questionEn,
            situationKo: situationKo,
            keywords: keywords,
          ),
        );
      }

      final week = _content?.week ?? DateTime.now().millisecondsSinceEpoch;
      final themes = _content?.themes ?? const <ThemeDef>[];
      final scenariosByThemeAndLevel =
          Map<String, Map<String, List<Scenario>>>.from(
            _content?.scenariosByThemeAndLevel ?? {},
          );
      scenariosByThemeAndLevel[themeId] = Map<String, List<Scenario>>.from(
        scenariosByThemeAndLevel[themeId] ?? {},
      );
      scenariosByThemeAndLevel[themeId]![levelId(level)] = list;
      _content = WeeklyContent(
        week: week,
        themes: themes,
        scenariosByThemeAndLevel: scenariosByThemeAndLevel,
      );
      notifyListeners();
      await _persistCache();
      return totalWritten;
    } catch (e) {
      debugPrint('[Scenario_Cache] err=$e');
      if (_ai.lastHttpStatus == 429) {
        _scenarioGenCooldownUntil[key] = DateTime.now().add(
          _scenarioCooldownDuration,
        );
      }
      return 0;
    }
  }

  Future<void> prefetchScenarios({
    required String themeId,
    required SurvivalLevel level,
    int minCount = 10,
  }) async {
    final pool = getScenarios(themeId, level);
    if (pool.length >= minCount) return;
    unawaited(
      ensureScenarioCache(themeId: themeId, level: level, minCount: minCount),
    );
  }

  Future<void> initAndRefresh() async {
    _loading = true;
    _error = null;
    notifyListeners();

    final prefs = await SharedPreferences.getInstance();

    final cachedJson = prefs.getString(_prefsCacheKey);
    if (cachedJson != null && cachedJson.trim().isNotEmpty) {
      final decoded = jsonDecode(cachedJson) as Map<String, dynamic>;
      final cached = WeeklyContent.fromJson(decoded);
      if (cached != null) {
        _content = cached;
        _loading = false;
        notifyListeners();
      }
    }

    try {
      final remoteWeek = await _fetchRemoteWeek();
      final localWeek = prefs.getInt(_prefsWeekKey) ?? _content?.week ?? 0;

      if (_content != null && remoteWeek == localWeek) {
        _loading = false;
        notifyListeners();
        return;
      }

      final fresh = await _downloadWeeklyContent(remoteWeek);
      _content = fresh;
      await prefs.setInt(_prefsWeekKey, fresh.week);
      await prefs.setString(_prefsCacheKey, jsonEncode(fresh.toJson()));
      _loading = false;
      notifyListeners();
    } catch (e) {
      _error = e.toString();
      _loading = false;
      notifyListeners();
    }
  }

  Future<int> _fetchRemoteWeek() async {
    final doc = await FirebaseFirestore.instance
        .collection('content_meta')
        .doc('current')
        .get();
    final data = doc.data();
    if (data == null) return DateTime.now().millisecondsSinceEpoch;
    final raw = data['week'];
    if (raw is int) return raw;
    if (raw is num) return raw.toInt();
    final week = int.tryParse((raw ?? '0').toString());
    return week ?? DateTime.now().millisecondsSinceEpoch;
  }

  Future<WeeklyContent> _downloadWeeklyContent(int week) async {
    final themeSnap = await FirebaseFirestore.instance
        .collection('themes')
        .get();
    final themes = <ThemeDef>[];
    for (final doc in themeSnap.docs) {
      final data = doc.data();
      final label = (data['label_ko'] ?? data['label'] ?? doc.id).toString();
      final emoji = (data['emoji'] ?? '🎭').toString();
      themes.add(ThemeDef(id: doc.id, label: label, emoji: emoji));
    }

    if (themes.isEmpty) {
      themes.addAll(const [
        ThemeDef(id: 'cafe', label: '카페', emoji: '☕️'),
        ThemeDef(id: 'airport', label: '공항', emoji: '🛫'),
        ThemeDef(id: 'hotel', label: '호텔', emoji: '🏨'),
        ThemeDef(id: 'clothing', label: '옷가게', emoji: '🛍️'),
        ThemeDef(id: 'workplace', label: '사회생활', emoji: '💼'),
        ThemeDef(id: 'neighborhood', label: '이웃', emoji: '🏘️'),
        ThemeDef(id: 'directions', label: '길찾기', emoji: '📍'),
        ThemeDef(id: 'interview', label: '면접', emoji: '🎤'),
        ThemeDef(id: 'party', label: '파티', emoji: '🥳'),
        ThemeDef(id: 'hospital', label: '병원', emoji: '🏥'),
      ]);
    }

    final scenarios = <String, Map<String, List<Scenario>>>{};
    for (final t in themes) {
      scenarios[t.id] = {};
      for (final level in SurvivalLevel.values) {
        final q = await FirebaseFirestore.instance
            .collection('themes')
            .doc(t.id)
            .collection('scenarios')
            .where('level', isEqualTo: levelId(level))
            .get();

        final list = <Scenario>[];
        for (final d in q.docs) {
          final data = d.data();
          final title = (data['title_ko'] ?? data['title'] ?? d.id).toString();
          final npcRole = (data['npcRole'] ?? data['npc_role'] ?? 'staff')
              .toString();
          final situation = (data['situation'] ?? '').toString();
          final questionEn = (data['question_en'] ?? '').toString();
          final situationKo = (data['situation_ko'] ?? '').toString();
          final keywords = ((data['keywords'] as List?) ?? const [])
              .map((e) => e.toString())
              .toList();
          list.add(
            Scenario(
              id: d.id,
              title: title,
              npcRole: npcRole,
              situation: situation,
              questionEn: questionEn,
              situationKo: situationKo,
              keywords: keywords,
            ),
          );
        }

        if (list.isNotEmpty) {
          scenarios[t.id]![levelId(level)] = list;
        }
      }
    }

    return WeeklyContent(
      week: week,
      themes: themes,
      scenariosByThemeAndLevel: scenarios,
    );
  }

  List<ThemeDef> getThemes() {
    return _content?.themes ?? const [];
  }

  List<Scenario> getScenarios(String themeId, SurvivalLevel level) {
    final fromRemote =
        _content?.scenariosByThemeAndLevel[themeId]?[levelId(level)];
    if (fromRemote != null && fromRemote.isNotEmpty) return fromRemote;
    return fallbackScenariosForTheme(themeId);
  }
}

final FirestoreContentStore contentStore = FirestoreContentStore();

class AppConfigStore extends ChangeNotifier {
  static const String _docPathCollection = 'app_config';
  static const String _docId = 'current';

  Map<String, dynamic> _strings = const {};
  Map<String, dynamic> _prompts = const {};
  bool _loading = true;
  String? _error;

  bool get loading => _loading;
  String? get error => _error;

  String t(String key) {
    final v = _strings[key];
    if (v is String && v.trim().isNotEmpty) return v;
    switch (key) {
      case 'app.title':
        return '3초 생존 영어';
      case 'common.retry':
        return '다시 시도';
      case 'error.content_load_title':
        return '컨텐츠 로딩이 잠깐 꼬였어요';
      case 'lobby.select_theme':
        return '테마 선택';
      default:
        return key;
    }
  }

  String? prompt(String key) {
    final v = _prompts[key];
    if (v is String && v.trim().isNotEmpty) return v;
    return null;
  }

  Future<void> initAndRefresh() async {
    _loading = true;
    _error = null;
    notifyListeners();

    try {
      final doc = await FirebaseFirestore.instance
          .collection(_docPathCollection)
          .doc(_docId)
          .get();
      final data = doc.data() ?? const <String, dynamic>{};
      _strings = Map<String, dynamic>.from(
        (data['strings'] as Map?) ?? const <String, dynamic>{},
      );
      _prompts = Map<String, dynamic>.from(
        (data['prompts'] as Map?) ?? const <String, dynamic>{},
      );
      _loading = false;
      notifyListeners();
    } catch (e) {
      _error = e.toString();
      _loading = false;
      notifyListeners();
    }
  }
}

final AppConfigStore appConfigStore = AppConfigStore();

class GeminiFailureFeedback {
  final String idealAnswer;
  final String idealAnswerKorean;
  final String tipKorean;
  final String reasonKorean;
  const GeminiFailureFeedback({
    required this.idealAnswer,
    required this.idealAnswerKorean,
    required this.tipKorean,
    required this.reasonKorean,
  });
}

class GeminiTurnJudgement {
  final int contextScore;
  final bool relevant;
  final String reasonKorean;
  final String npcNextLine;
  final String idealAnswer;
  final String idealAnswerKorean;
  final String tipKorean;
  const GeminiTurnJudgement({
    required this.contextScore,
    required this.relevant,
    required this.reasonKorean,
    required this.npcNextLine,
    required this.idealAnswer,
    required this.idealAnswerKorean,
    required this.tipKorean,
  });
}

abstract class AiClient {
  int? get lastHttpStatus;
  String? get lastHttpBody;
  String? get lastException;

  Future<String?> generateOpeningLine({
    required ThemeDef theme,
    required SurvivalLevel level,
    required Scenario scenario,
  });

  Future<List<Map<String, dynamic>>?> generateScenarioBatch({
    required ThemeDef theme,
    required SurvivalLevel level,
    required int count,
    required List<String> recentSituationHints,
  });

  Future<String?> translateToKorean(String english);

  Future<GeminiFailureFeedback?> generateFailureFeedback({
    required ThemeDef theme,
    required SurvivalLevel level,
    required Scenario scenario,
    required String npcLine,
    required String userAnswer,
    required int passThreshold,
    required String failureReasonHintKorean,
  });

  Future<GeminiTurnJudgement?> judgeAndNext({
    required ThemeDef theme,
    required SurvivalLevel level,
    required int passThreshold,
    required Scenario scenario,
    required List<Map<String, String>> history,
    required String npcLine,
    required String userAnswer,
  });
}

class FailoverAiClient implements AiClient {
  final AiClient primary;
  final AiClient secondary;

  int? _lastStatus;
  String? _lastBody;
  String? _lastException;
  String? _lastDebugSummary;

  FailoverAiClient({required this.primary, required this.secondary});

  String? get lastDebugSummary => _lastDebugSummary;

  bool _shouldFailover(AiClient client) {
    final status = client.lastHttpStatus;
    if (status == 429) return true;
    if (status == 408) return true;
    if (status == 401) return true;
    if (status == 403) return true;
    if (status == 404) return true;
    if (status != null && status >= 500 && status <= 599) return true;
    if ((client.lastException ?? '').trim().isNotEmpty) return true;

    // If we got a 2xx but still returned null, it's most likely a parse/schema
    // failure. Allow immediate failover to keep the dialogue alive.
    if (status != null && status >= 200 && status <= 299) return true;

    // If the operation returned null but we have no diagnostic status/exception,
    // treat it as an unknown failure and allow failover.
    if (status == null && (client.lastException ?? '').trim().isEmpty) {
      return true;
    }
    return false;
  }

  void _captureLast(AiClient client) {
    _lastStatus = client.lastHttpStatus;
    _lastBody = client.lastHttpBody;
    _lastException = client.lastException;
  }

  String _clientLabel(AiClient c) {
    final name = c.runtimeType.toString();
    if (name.toLowerCase().contains('gemini')) return 'Gemini';
    if (name.toLowerCase().contains('openai')) return 'OpenAI';
    return name;
  }

  String _clip(String? s, int max) {
    final v = (s ?? '').toString();
    if (v.isEmpty) return '';
    return v.length > max ? v.substring(0, max) : v;
  }

  void _setDebugSummary({
    required String op,
    required AiClient p,
    required AiClient s,
    required bool triedSecondary,
    required bool ok,
  }) {
    final pLabel = _clientLabel(p);
    final sLabel = _clientLabel(s);
    final pStatus = p.lastHttpStatus;
    final sStatus = s.lastHttpStatus;
    final pEx = (p.lastException ?? '').trim();
    final sEx = (s.lastException ?? '').trim();
    final pBody = _clip(p.lastHttpBody, 220);
    final sBody = _clip(s.lastHttpBody, 220);

    final parts = <String>[];
    parts.add('op=$op ok=$ok');
    parts.add(
      'primary=$pLabel status=${pStatus ?? '-'} ex=${pEx.isEmpty ? '-' : pEx}',
    );
    if (pBody.isNotEmpty) parts.add('p_body="$pBody"');
    parts.add('failover=$triedSecondary');
    if (triedSecondary) {
      parts.add(
        'secondary=$sLabel status=${sStatus ?? '-'} ex=${sEx.isEmpty ? '-' : sEx}',
      );
      if (sBody.isNotEmpty) parts.add('s_body="$sBody"');
    }
    _lastDebugSummary = parts.join(' | ');
  }

  @override
  int? get lastHttpStatus => _lastStatus;
  @override
  String? get lastHttpBody => _lastBody;
  @override
  String? get lastException => _lastException;

  @override
  Future<String?> generateOpeningLine({
    required ThemeDef theme,
    required SurvivalLevel level,
    required Scenario scenario,
  }) async {
    final r1 = await primary.generateOpeningLine(
      theme: theme,
      level: level,
      scenario: scenario,
    );
    _captureLast(primary);
    if (r1 != null) {
      debugPrint(
        '[Opener_Proof] client=${_clientLabel(primary)} theme=${theme.id} level=${levelId(level)} scenario_id=${scenario.id} generated="${r1.replaceAll(RegExp(r"\s+"), " ").trim()}"',
      );
      _setDebugSummary(
        op: 'opener',
        p: primary,
        s: secondary,
        triedSecondary: false,
        ok: true,
      );
      return r1;
    }
    if (!_shouldFailover(primary)) {
      _setDebugSummary(
        op: 'opener',
        p: primary,
        s: secondary,
        triedSecondary: false,
        ok: false,
      );
      return null;
    }
    debugPrint(
      '[AI_Failover] opener primary_failed status=${primary.lastHttpStatus} ex=${primary.lastException}',
    );
    final r2 = await secondary.generateOpeningLine(
      theme: theme,
      level: level,
      scenario: scenario,
    );
    _captureLast(secondary);
    if (r2 != null) {
      debugPrint(
        '[Opener_Proof] client=${_clientLabel(secondary)} theme=${theme.id} level=${levelId(level)} scenario_id=${scenario.id} generated="${r2.replaceAll(RegExp(r"\s+"), " ").trim()}"',
      );
    }
    debugPrint(
      '[AI_Failover] opener secondary_status=${secondary.lastHttpStatus} ex=${secondary.lastException} ok=${r2 != null}',
    );
    _setDebugSummary(
      op: 'opener',
      p: primary,
      s: secondary,
      triedSecondary: true,
      ok: r2 != null,
    );
    return r2;
  }

  @override
  Future<List<Map<String, dynamic>>?> generateScenarioBatch({
    required ThemeDef theme,
    required SurvivalLevel level,
    required int count,
    required List<String> recentSituationHints,
  }) async {
    final r1 = await primary.generateScenarioBatch(
      theme: theme,
      level: level,
      count: count,
      recentSituationHints: recentSituationHints,
    );
    _captureLast(primary);
    if (r1 != null) {
      _setDebugSummary(
        op: 'scenario_batch',
        p: primary,
        s: secondary,
        triedSecondary: false,
        ok: true,
      );
      return r1;
    }
    if (!_shouldFailover(primary)) {
      _setDebugSummary(
        op: 'scenario_batch',
        p: primary,
        s: secondary,
        triedSecondary: false,
        ok: false,
      );
      return null;
    }
    debugPrint(
      '[AI_Failover] scenario_batch primary_failed status=${primary.lastHttpStatus} ex=${primary.lastException}',
    );
    final r2 = await secondary.generateScenarioBatch(
      theme: theme,
      level: level,
      count: count,
      recentSituationHints: recentSituationHints,
    );
    _captureLast(secondary);
    debugPrint(
      '[AI_Failover] scenario_batch secondary_status=${secondary.lastHttpStatus} ex=${secondary.lastException} ok=${r2 != null}',
    );
    _setDebugSummary(
      op: 'scenario_batch',
      p: primary,
      s: secondary,
      triedSecondary: true,
      ok: r2 != null,
    );
    return r2;
  }

  @override
  Future<String?> translateToKorean(String english) async {
    final r1 = await primary.translateToKorean(english);
    _captureLast(primary);
    if (r1 != null) {
      _setDebugSummary(
        op: 'translate',
        p: primary,
        s: secondary,
        triedSecondary: false,
        ok: true,
      );
      return r1;
    }
    if (!_shouldFailover(primary)) {
      _setDebugSummary(
        op: 'translate',
        p: primary,
        s: secondary,
        triedSecondary: false,
        ok: false,
      );
      return null;
    }
    debugPrint(
      '[AI_Failover] translate primary_failed status=${primary.lastHttpStatus} ex=${primary.lastException}',
    );
    final r2 = await secondary.translateToKorean(english);
    _captureLast(secondary);
    debugPrint(
      '[AI_Failover] translate secondary_status=${secondary.lastHttpStatus} ex=${secondary.lastException} ok=${r2 != null}',
    );
    _setDebugSummary(
      op: 'translate',
      p: primary,
      s: secondary,
      triedSecondary: true,
      ok: r2 != null,
    );
    return r2;
  }

  @override
  Future<GeminiFailureFeedback?> generateFailureFeedback({
    required ThemeDef theme,
    required SurvivalLevel level,
    required Scenario scenario,
    required String npcLine,
    required String userAnswer,
    required int passThreshold,
    required String failureReasonHintKorean,
  }) async {
    final r1 = await primary.generateFailureFeedback(
      theme: theme,
      level: level,
      scenario: scenario,
      npcLine: npcLine,
      userAnswer: userAnswer,
      passThreshold: passThreshold,
      failureReasonHintKorean: failureReasonHintKorean,
    );
    _captureLast(primary);
    if (r1 != null) {
      _setDebugSummary(
        op: 'failure_feedback',
        p: primary,
        s: secondary,
        triedSecondary: false,
        ok: true,
      );
      return r1;
    }
    if (!_shouldFailover(primary)) {
      _setDebugSummary(
        op: 'failure_feedback',
        p: primary,
        s: secondary,
        triedSecondary: false,
        ok: false,
      );
      return null;
    }
    debugPrint(
      '[AI_Failover] failure_feedback primary_failed status=${primary.lastHttpStatus} ex=${primary.lastException}',
    );
    final r2 = await secondary.generateFailureFeedback(
      theme: theme,
      level: level,
      scenario: scenario,
      npcLine: npcLine,
      userAnswer: userAnswer,
      passThreshold: passThreshold,
      failureReasonHintKorean: failureReasonHintKorean,
    );
    _captureLast(secondary);
    debugPrint(
      '[AI_Failover] failure_feedback secondary_status=${secondary.lastHttpStatus} ex=${secondary.lastException} ok=${r2 != null}',
    );
    _setDebugSummary(
      op: 'failure_feedback',
      p: primary,
      s: secondary,
      triedSecondary: true,
      ok: r2 != null,
    );
    return r2;
  }

  @override
  Future<GeminiTurnJudgement?> judgeAndNext({
    required ThemeDef theme,
    required SurvivalLevel level,
    required int passThreshold,
    required Scenario scenario,
    required List<Map<String, String>> history,
    required String npcLine,
    required String userAnswer,
  }) async {
    final r1 = await primary.judgeAndNext(
      theme: theme,
      level: level,
      passThreshold: passThreshold,
      scenario: scenario,
      history: history,
      npcLine: npcLine,
      userAnswer: userAnswer,
    );
    _captureLast(primary);
    if (r1 != null) {
      _setDebugSummary(
        op: 'judge',
        p: primary,
        s: secondary,
        triedSecondary: false,
        ok: true,
      );
      return r1;
    }
    if (!_shouldFailover(primary)) {
      _setDebugSummary(
        op: 'judge',
        p: primary,
        s: secondary,
        triedSecondary: false,
        ok: false,
      );
      return null;
    }
    debugPrint(
      '[AI_Failover] judge primary_failed status=${primary.lastHttpStatus} ex=${primary.lastException}',
    );
    final r2 = await secondary.judgeAndNext(
      theme: theme,
      level: level,
      passThreshold: passThreshold,
      scenario: scenario,
      history: history,
      npcLine: npcLine,
      userAnswer: userAnswer,
    );
    _captureLast(secondary);
    debugPrint(
      '[AI_Failover] judge secondary_status=${secondary.lastHttpStatus} ex=${secondary.lastException} ok=${r2 != null}',
    );
    _setDebugSummary(
      op: 'judge',
      p: primary,
      s: secondary,
      triedSecondary: true,
      ok: r2 != null,
    );
    return r2;
  }
}

class OpenAiAiClient implements AiClient {
  int? _lastHttpStatus;
  String? _lastHttpBody;
  String? _lastException;

  @override
  int? get lastHttpStatus => _lastHttpStatus;
  @override
  String? get lastHttpBody => _lastHttpBody;
  @override
  String? get lastException => _lastException;

  void _setHttp({int? status, String? body, String? exception}) {
    _lastHttpStatus = status;
    _lastHttpBody = body;
    _lastException = exception;
  }

  String _stripCodeFences(String s) {
    var out = s.trim();
    if (out.startsWith('```')) {
      out = out.replaceAll(RegExp(r'^```[a-zA-Z]*\n'), '');
      out = out.replaceAll(RegExp(r'```\s*$'), '');
    }
    return out.trim();
  }

  String? _extractFirstJsonObject(String s) {
    final text = _stripCodeFences(s);
    final start = text.indexOf('{');
    if (start < 0) return null;
    var depth = 0;
    for (int i = start; i < text.length; i++) {
      final ch = text[i];
      if (ch == '{') depth++;
      if (ch == '}') {
        depth--;
        if (depth == 0) {
          return text.substring(start, i + 1);
        }
      }
    }
    final end = text.lastIndexOf('}');
    if (end > start) return text.substring(start, end + 1);
    return null;
  }

  Future<String?> _chat({
    required String prompt,
    double temperature = 0.7,
    int maxOutputTokens = 256,
  }) async {
    if (openAiApiKey.trim().isEmpty) return null;
    try {
      final uri = Uri.parse('https://api.openai.com/v1/chat/completions');
      final res = await http
          .post(
            uri,
            headers: {
              'Authorization': 'Bearer $openAiApiKey',
              'Content-Type': 'application/json',
            },
            body: jsonEncode({
              'model': 'gpt-4o-mini',
              'temperature': temperature,
              'max_tokens': maxOutputTokens,
              'messages': [
                {'role': 'user', 'content': prompt},
              ],
            }),
          )
          .timeout(const Duration(seconds: 18));

      _setHttp(status: res.statusCode, body: res.body, exception: null);
      if (res.statusCode < 200 || res.statusCode >= 300) {
        final raw = res.body.toString();
        final clipped = raw.length > 600 ? raw.substring(0, 600) : raw;
        debugPrint('[OpenAI_HTTP] status=${res.statusCode} body="$clipped"');
        return null;
      }
      final data = jsonDecode(res.body);
      final content = data['choices']?[0]?['message']?['content']
          ?.toString()
          .trim();
      if (content == null || content.isEmpty) return null;
      return content;
    } catch (e) {
      _setHttp(status: null, body: null, exception: e.toString());
      debugPrint('[OpenAI_HTTP] exception err=$e');
      return null;
    }
  }

  String _scenarioPrompt({
    required ThemeDef theme,
    required SurvivalLevel level,
    required int count,
    required List<String> recentSituationHints,
  }) {
    final lvl = levelLabel(level);
    final persona = switch (level) {
      SurvivalLevel.beginner =>
        'Easy real-talk words. Normal pace. Keep it simple. Intent-first.',
      SurvivalLevel.intermediate =>
        'Everyday natural speech with contractions. Normal pace. Include ONE everyday idiom naturally.',
      SurvivalLevel.advanced =>
        'Richer idioms and emotion. Faster pace feel. Longer natural sentences.',
      SurvivalLevel.hardcore =>
        'No constraints. Rapid-fire, heavy contractions, improvised persona. Extreme realism.',
    };
    final avoid = recentSituationHints
        .where((e) => e.trim().isNotEmpty)
        .take(20);
    return """
You are a content generator for a 3-second survival English roleplay game.

Theme: ${theme.label}
Level: $lvl
Level style: $persona

Generate $count DIFFERENT scenario items for this theme.

Self-correction (mandatory):
- After drafting the scenarios, re-read EACH sentence and fix grammar, unnatural phrasing, and awkward word choice.
- Remove any inappropriate content (hate/harassment/sexual/minors/violence/self-harm) and rewrite into a safe everyday situation.
- Ensure there are no swear words or slurs.

Hard constraints:
- Each scenario MUST be a distinct micro-topic within the theme (avoid repetition).
- Do NOT repeat these recent situations (avoid similar phrasing/topics):
${avoid.isEmpty ? '(none)' : avoid.map((e) => '- $e').join('\n')}
- Scenarios must be realistic everyday situations.
- Return STRICT JSON only.

Output format: JSON array of objects.
Each object MUST have:
{
  "title_ko": "...",
  "npcRole": "...",
  "situation": "...",
  "situation_ko": "...",
  "question_en": "...",
  "keywords": ["...", "..."]
}
Return ONLY the JSON array.
""";
  }

  @override
  Future<List<Map<String, dynamic>>?> generateScenarioBatch({
    required ThemeDef theme,
    required SurvivalLevel level,
    required int count,
    required List<String> recentSituationHints,
  }) async {
    final text = await _chat(
      prompt: _scenarioPrompt(
        theme: theme,
        level: level,
        count: count,
        recentSituationHints: recentSituationHints,
      ),
      temperature: 0.9,
      maxOutputTokens: 1400,
    );
    if (text == null || text.trim().isEmpty) return null;
    try {
      final decoded = jsonDecode(text);
      if (decoded is! List) return null;
      return decoded
          .whereType<dynamic>()
          .map((e) => Map<String, dynamic>.from(e as Map))
          .toList();
    } catch (_) {
      return null;
    }
  }

  @override
  Future<String?> generateOpeningLine({
    required ThemeDef theme,
    required SurvivalLevel level,
    required Scenario scenario,
  }) async {
    final lvl = levelLabel(level);
    final persona = switch (level) {
      SurvivalLevel.beginner =>
        'Beginner(초급): Friendly but realistic local. Real-talk with EASY words. Normal pace vibe. 1 sentence. Avoid textbook robot lines.',
      SurvivalLevel.intermediate =>
        'Intermediate(중급): Everyday local. Normal pace vibe. Use natural contractions/linking (gonna/wanna/kinda) and ONE everyday idiom naturally.',
      SurvivalLevel.advanced =>
        'Advanced(고급): Expressive native. Fast pace vibe. Richer idioms + emotion + nuance. Slightly longer natural sentence.',
      SurvivalLevel.hardcore =>
        'Extreme(최상급): No-filter reality. Native sprint vibe. Heavy contractions/slang/ellipsis. Improvised persona (busy/noisy). Must feel very real.',
    };
    final prompt =
        """
You are an NPC in a 3-second survival English game.

Theme: ${theme.label}
Level: $lvl
Level style: $persona
NPC role: ${scenario.npcRole}
Situation: ${scenario.situation}

Task:
Write ONE short natural opening line in English (1 sentence) that starts this roleplay.
You MUST vary phrasing across runs to prevent memorization.
No quotes. No extra text.
""";
    final text = await _chat(
      prompt: prompt,
      temperature: 0.9,
      maxOutputTokens: 80,
    );
    final out = text?.replaceAll(RegExp(r'\s+'), ' ').trim();
    if (out != null && out.isNotEmpty) {
      debugPrint(
        '[OpenAI_Opener] theme=${theme.id} level=${levelId(level)} scenario_id=${scenario.id} generated="$out"',
      );
    }
    return (out == null || out.isEmpty) ? null : out;
  }

  @override
  Future<String?> translateToKorean(String english) async {
    final s = english.trim();
    if (s.isEmpty) return null;
    final prompt =
        """
Translate the following English sentence into natural Korean.
Return ONLY the Korean translation. No quotes. No extra text.

English:
$s
""";
    final text = await _chat(
      prompt: prompt,
      temperature: 0.2,
      maxOutputTokens: 120,
    );
    final out = text?.trim();
    return (out == null || out.isEmpty) ? null : out;
  }

  @override
  Future<GeminiFailureFeedback?> generateFailureFeedback({
    required ThemeDef theme,
    required SurvivalLevel level,
    required Scenario scenario,
    required String npcLine,
    required String userAnswer,
    required int passThreshold,
    required String failureReasonHintKorean,
  }) async {
    final lvl = levelLabel(level);
    final hint = failureReasonHintKorean.trim();
    final allowedTipWords = <String>{
      ...scenario.keywords.map((e) => e.toLowerCase().trim()),
      ...scenario.questionEn
          .toLowerCase()
          .replaceAll(RegExp(r"[^a-z0-9'\s]"), ' ')
          .split(RegExp(r'\s+'))
          .map((e) => e.trim())
          .where((e) => e.length >= 2),
    }.where((e) => e.isNotEmpty).toList()..sort();

    final prompt =
        """
You are a strict but helpful English coach for a 3-second survival English game.

Theme: ${theme.label}
Level: $lvl
NPC role: ${scenario.npcRole}
Situation: ${scenario.situation}

NPC line: $npcLine
User answer (ASR transcription): $userAnswer
Pass threshold (context score): $passThreshold

Failure hint (Korean): ${hint.isEmpty ? "(none)" : hint}

Task:
Generate a coaching set in JSON. Make it short and practical.

Tip constraints (anti-hallucination):
- In tip_ko, you MUST NOT introduce any new English keywords.
- If you include any English words, they MUST come from this allowlist only:
${allowedTipWords.isEmpty ? '(none)' : allowedTipWords.map((e) => '- $e').join('\n')}
- If you cannot comply, write tip_ko without English words.

Output MUST be strict JSON:
{
  "ideal_answer": "...",
  "ideal_answer_ko": "...",
  "tip_ko": "...",
  "reason_ko": "..."
}
Return ONLY JSON.
""";
    final text = await _chat(
      prompt: prompt,
      temperature: 0.7,
      maxOutputTokens: 260,
    );
    if (text == null || text.trim().isEmpty) return null;
    try {
      final Map<String, dynamic> data = jsonDecode(text);
      final ideal = (data['ideal_answer'] ?? '').toString().trim();
      final idealKo = (data['ideal_answer_ko'] ?? '').toString().trim();
      final tipKo = (data['tip_ko'] ?? '').toString().trim();
      final reasonKo = (data['reason_ko'] ?? '').toString().trim();
      if (ideal.isEmpty ||
          idealKo.isEmpty ||
          tipKo.isEmpty ||
          reasonKo.isEmpty) {
        return null;
      }
      return GeminiFailureFeedback(
        idealAnswer: ideal,
        idealAnswerKorean: idealKo,
        tipKorean: tipKo,
        reasonKorean: reasonKo,
      );
    } catch (_) {
      return null;
    }
  }

  @override
  Future<GeminiTurnJudgement?> judgeAndNext({
    required ThemeDef theme,
    required SurvivalLevel level,
    required int passThreshold,
    required Scenario scenario,
    required List<Map<String, String>> history,
    required String npcLine,
    required String userAnswer,
  }) async {
    final lvl = levelLabel(level);
    final persona = switch (level) {
      SurvivalLevel.beginner =>
        'Level 1 (초급 - Beginner): "You are a very kind, patient native speaker. 유저가 단어 하나만 짧게 말해도 의도를 100% 긍정적으로 유추해서 친절하게 통과시키고, 대화를 이어갈 수 있는 아주 쉬운 질문을 던져라."',
      SurvivalLevel.intermediate =>
        'Level 2 (중급 - Intermediate): "You are a standard clerk/native speaker. 유저가 너무 짧게 답하면 통과는 시켜주되, \'What size?\', \'Anything else?\'처럼 구체적인 정보를 한 가지 더 물어봐라."',
      SurvivalLevel.advanced =>
        'Level 3 (고급 - Advanced): "You are a busy native speaker. 대화 중간에 반드시 \'돌발 상황(Unexpected variables)\'을 하나 던져라. (예: 메뉴 품절, 자리 없음, 카드 결제기 고장 등) 유저가 문장으로 상황을 모면하도록 유도해라."',
      SurvivalLevel.hardcore =>
        'Level 4 (최상급 - Master): "You are a very rushed, impatient native speaker in a bustling environment. 말을 빠르고 압박하듯이 하고(Time pressure), 원어민들이 쓰는 구어체나 슬랭을 섞어 써라. 유저가 빨리 결정하지 않으면 곤란해하는 티를 내라."',
    };

    final l4PressureRule = level == SurvivalLevel.hardcore
        ? '- MASTER HARD RULE: npc_next MUST include at least ONE explicit time-pressure/impatience word: "hurry" or "quick" or "busy" or "slammed" or "next in line".'
        : '';
    final l4JsonStrict = level == SurvivalLevel.hardcore
        ? 'MASTER JSON RULE (non-negotiable): Even if you roleplay as impatient, you MUST output a complete and valid JSON object ONLY. Include ALL required keys. Do not output empty strings for npc_next/ideal_answer.'
        : '';
    final allowedTipWords = <String>{
      ...scenario.keywords.map((e) => e.toLowerCase().trim()),
      ...scenario.questionEn
          .toLowerCase()
          .replaceAll(RegExp(r"[^a-z0-9'\s]"), ' ')
          .split(RegExp(r'\s+'))
          .map((e) => e.trim())
          .where((e) => e.length >= 2),
    }.where((e) => e.isNotEmpty).toList()..sort();

    final prompt =
        """
You are the sole judge and dialogue engine for a 3-second survival English game.

Theme: ${theme.label}
Level: $lvl
NPC role: ${scenario.npcRole}
Situation: ${scenario.situation}

Conversation so far (most recent last):
${history.map((e) => "${e['role']}: ${e['text']}").join("\n")}

NPC line (current): $npcLine
User answer (ASR transcription): $userAnswer

Core policy (SURVIVALISM):
- If the meaning/intention is understandable in context, survival should be granted even with broken grammar or awkward phrasing.
- Only treat as failure when intent is clearly unrelated OR meaning is severely distorted by misunderstanding.
- IMPORTANT: Do NOT auto-pass empty, silence, or gibberish.
- If user answer is empty/meaningless (e.g., only "...", random letters, or unrelated filler), set relevant=false and context_score well below threshold.

Level policy:
$persona

Scoring rule:
- Return context_score 0-100.
- Set relevant=true ONLY if context_score >= $passThreshold.
- IMPORTANT for Beginner: If intent is understandable, you MUST set context_score >= $passThreshold.

Task:
1) Decide survival (relevant).
2) Generate the next NPC line as a natural tail question or response that continues the conversation.
3) Provide coaching set (ideal answer + Korean meaning + one-line Korean tip).
4) Provide ONE-LINE Korean reason.

$l4JsonStrict

NPC_NEXT rules (very important):
- npc_next MUST directly react to the user's answer and the immediate context.
- npc_next MUST include at least ONE key word/requirement from the user's answer (e.g., menu name like "coffee", or a request like "no sugar").
- Reuse the user's key word(s) verbatim (case-insensitive match). Do NOT replace with a synonym.
- Do NOT ask a generic random question. Do NOT reset the topic.
- Prefer a short, natural follow-up question (1 sentence) that a real person would say next.
- If the user answered with a preference/choice, ask a specific next-step question based on it.
- If the user complained/asked for help, respond with a realistic service reply and ask one clarifying question.
- Avoid repeating the same wording as the previous NPC line.
$l4PressureRule

Tip constraints (anti-hallucination):
- In tip_ko, you MUST NOT introduce any new English keywords.
- If you include any English words, they MUST come from this allowlist only:
${allowedTipWords.isEmpty ? '(none)' : allowedTipWords.map((e) => '- $e').join('\n')}
- If you cannot comply, write tip_ko without English words.

Output MUST be strict JSON:
{
  "context_score": 0,
  "relevant": true,
  "reason_ko": "...",
  "npc_next": "...",
  "ideal_answer": "...",
  "ideal_answer_ko": "...",
  "tip_ko": "..."
}
Return ONLY JSON.
""";

    GeminiTurnJudgement? parseTurn(String raw) {
      Map<String, dynamic> data;
      try {
        data = Map<String, dynamic>.from(jsonDecode(_stripCodeFences(raw)));
      } catch (_) {
        final extracted = _extractFirstJsonObject(raw);
        if (extracted == null) return null;
        try {
          data = Map<String, dynamic>.from(jsonDecode(extracted));
        } catch (_) {
          return null;
        }
      }

      final score = int.tryParse('${data['context_score']}') ?? 0;
      final relevant =
          (data['relevant'] == true) ||
          (data['relevant']?.toString().toLowerCase() == 'true');
      final reasonKo = (data['reason_ko'] ?? '').toString().trim();
      final npcNext = (data['npc_next'] ?? '').toString().trim();
      final ideal = (data['ideal_answer'] ?? '').toString().trim();
      final idealKo = (data['ideal_answer_ko'] ?? '').toString().trim();
      final tipKo = (data['tip_ko'] ?? '').toString().trim();
      if (reasonKo.isEmpty ||
          npcNext.isEmpty ||
          ideal.isEmpty ||
          idealKo.isEmpty) {
        return null;
      }
      return GeminiTurnJudgement(
        contextScore: score.clamp(0, 100),
        relevant: relevant,
        reasonKorean: reasonKo,
        npcNextLine: npcNext,
        idealAnswer: ideal,
        idealAnswerKorean: idealKo,
        tipKorean: tipKo.isEmpty ? '의미가 통하면 OK. 핵심 정보만 또렷하게!' : tipKo,
      );
    }

    final text = await _chat(
      prompt: prompt,
      temperature: 0.7,
      maxOutputTokens: 520,
    );
    if (text == null || text.trim().isEmpty) return null;
    final first = parseTurn(text);
    if (first != null) return first;

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
Do not include any other text.

Previous output:
$text
""";

    final repaired = await _chat(
      prompt: repairPrompt,
      temperature: 0.2,
      maxOutputTokens: 520,
    );
    if (repaired == null || repaired.trim().isEmpty) return null;
    return parseTurn(repaired);
  }
}

class GeminiAiClient implements AiClient {
  final GeminiNpcService _svc;
  GeminiAiClient([GeminiNpcService? svc]) : _svc = svc ?? GeminiNpcService();

  @override
  int? get lastHttpStatus => _svc.lastHttpStatus;
  @override
  String? get lastHttpBody => _svc.lastHttpBody;
  @override
  String? get lastException => _svc.lastException;

  @override
  Future<String?> generateOpeningLine({
    required ThemeDef theme,
    required SurvivalLevel level,
    required Scenario scenario,
  }) {
    return _svc.generateOpeningLine(
      theme: theme,
      level: level,
      scenario: scenario,
    );
  }

  @override
  Future<List<Map<String, dynamic>>?> generateScenarioBatch({
    required ThemeDef theme,
    required SurvivalLevel level,
    required int count,
    required List<String> recentSituationHints,
  }) {
    return _svc.generateScenarioBatch(
      theme: theme,
      level: level,
      count: count,
      recentSituationHints: recentSituationHints,
    );
  }

  @override
  Future<String?> translateToKorean(String english) {
    return _svc.translateToKorean(english);
  }

  @override
  Future<GeminiFailureFeedback?> generateFailureFeedback({
    required ThemeDef theme,
    required SurvivalLevel level,
    required Scenario scenario,
    required String npcLine,
    required String userAnswer,
    required int passThreshold,
    required String failureReasonHintKorean,
  }) {
    return _svc.generateFailureFeedback(
      theme: theme,
      level: level,
      scenario: scenario,
      npcLine: npcLine,
      userAnswer: userAnswer,
      passThreshold: passThreshold,
      failureReasonHintKorean: failureReasonHintKorean,
    );
  }

  @override
  Future<GeminiTurnJudgement?> judgeAndNext({
    required ThemeDef theme,
    required SurvivalLevel level,
    required int passThreshold,
    required Scenario scenario,
    required List<Map<String, String>> history,
    required String npcLine,
    required String userAnswer,
  }) {
    return _svc.judgeAndNext(
      theme: theme,
      level: level,
      passThreshold: passThreshold,
      scenario: scenario,
      history: history,
      npcLine: npcLine,
      userAnswer: userAnswer,
    );
  }
}

class GeminiNpcService {
  static const List<String> _modelFallbacks = <String>['gemini-1.5-flash'];

  static String? _resolvedModel;
  static DateTime? _resolvedModelAt;

  // NOTE (critical): We intentionally hard-fix to a stable model to avoid 404s
  // from dynamic aliases like `*-latest`.
  // TODO: Later, allow overriding this via AppConfigStore/Remote Config so we
  // can switch models without an app update.
  static String get _preferredModel {
    final configured = (appConfigStore.prompt('gemini.model') ?? '').trim();
    if (configured.isNotEmpty) return configured;
    return _modelFallbacks.first;
  }

  static DateTime? _cooldownUntil;
  static const Duration _cooldownOn429 = Duration(seconds: 90);

  int? lastHttpStatus;
  String? lastHttpBody;
  String? lastException;

  String _stripCodeFences(String s) {
    var out = s.trim();
    if (out.startsWith('```')) {
      out = out.replaceAll(RegExp(r'^```[a-zA-Z]*\n'), '');
      out = out.replaceAll(RegExp(r'```\s*$'), '');
    }
    return out.trim();
  }

  String? _extractFirstJsonObject(String s) {
    final text = _stripCodeFences(s);
    final start = text.indexOf('{');
    if (start < 0) return null;
    var depth = 0;
    for (int i = start; i < text.length; i++) {
      final ch = text[i];
      if (ch == '{') depth++;
      if (ch == '}') {
        depth--;
        if (depth == 0) {
          return text.substring(start, i + 1);
        }
      }
    }
    final end = text.lastIndexOf('}');
    if (end > start) return text.substring(start, end + 1);
    return null;
  }

  Map<String, dynamic>? _parseJsonObject(String text) {
    try {
      return Map<String, dynamic>.from(jsonDecode(_stripCodeFences(text)));
    } catch (_) {
      final extracted = _extractFirstJsonObject(text);
      if (extracted == null) return null;
      try {
        return Map<String, dynamic>.from(jsonDecode(extracted));
      } catch (_) {
        return null;
      }
    }
  }

  List<String> _candidateModels() {
    final preferred = _preferredModel.trim();
    final resolved = (_resolvedModel ?? '').trim();
    final out = <String>[];
    if (resolved.isNotEmpty) out.add(resolved);
    if (preferred.isNotEmpty && !out.contains(preferred)) out.add(preferred);
    for (final m in _modelFallbacks) {
      final mm = m.trim();
      if (mm.isEmpty) continue;
      if (!out.contains(mm)) out.add(mm);
    }
    return out;
  }

  Future<String?> _resolveModelByListModels(String apiVersion) async {
    // Avoid spamming listModels.
    final at = _resolvedModelAt;
    if (at != null &&
        DateTime.now().difference(at) < const Duration(minutes: 10)) {
      final cached = (_resolvedModel ?? '').trim();
      return cached.isEmpty ? null : cached;
    }

    try {
      final uri = Uri.parse(
        'https://generativelanguage.googleapis.com/$apiVersion/models?key=$geminiApiKey',
      );
      final res = await http
          .get(uri, headers: {'Content-Type': 'application/json'})
          .timeout(const Duration(seconds: 18));
      if (res.statusCode < 200 || res.statusCode >= 300) {
        return null;
      }
      final decoded = jsonDecode(res.body) as Map<String, dynamic>;
      final models = (decoded['models'] as List?) ?? const [];
      if (models.isEmpty) return null;

      bool supportsGenerateContent(Map<String, dynamic> m) {
        final methods = (m['supportedGenerationMethods'] as List?) ?? const [];
        return methods.map((e) => e.toString()).contains('generateContent');
      }

      String normalizeName(String raw) {
        final s = raw.trim();
        if (s.startsWith('models/')) return s.substring('models/'.length);
        return s;
      }

      final parsed = models
          .whereType<Map>()
          .map((e) => Map<String, dynamic>.from(e))
          .where((m) => supportsGenerateContent(m))
          .toList();
      if (parsed.isEmpty) return null;

      Map<String, dynamic>? pickFirstWhere(
        bool Function(String nameLower) pred,
      ) {
        for (final m in parsed) {
          final name = (m['name'] ?? '').toString();
          final n = normalizeName(name);
          if (pred(n.toLowerCase())) return m;
        }
        return null;
      }

      final picked =
          pickFirstWhere((n) => n.contains('flash') && n.contains('1.5')) ??
          pickFirstWhere((n) => n.contains('flash')) ??
          parsed.first;

      final name = normalizeName((picked['name'] ?? '').toString());
      if (name.trim().isEmpty) return null;

      _resolvedModel = name.trim();
      _resolvedModelAt = DateTime.now();
      debugPrint(
        '[Gemini_HTTP] listModels resolved model=$_resolvedModel api=$apiVersion',
      );
      return _resolvedModel;
    } catch (_) {
      return null;
    }
  }

  Future<String?> generateOpeningLine({
    required ThemeDef theme,
    required SurvivalLevel level,
    required Scenario scenario,
  }) async {
    if (geminiApiKey.trim().isEmpty) return null;

    final prompt = _openingPrompt(
      theme: theme,
      level: level,
      scenario: scenario,
    );

    final text = await _generateText(
      prompt,
      temperature: 0.9,
      maxOutputTokens: 80,
    );
    if (text == null) return null;
    final out = text.replaceAll(RegExp(r'\s+'), ' ').trim();
    if (out.isEmpty) return null;
    debugPrint(
      '[Gemini_Opener] Level: ${levelLabel(level)}, source_id: ${scenario.id}, generated: "$out"',
    );
    return out;
  }

  Future<List<Map<String, dynamic>>?> generateScenarioBatch({
    required ThemeDef theme,
    required SurvivalLevel level,
    required int count,
    required List<String> recentSituationHints,
  }) async {
    if (geminiApiKey.trim().isEmpty) return null;
    final lvl = levelLabel(level);
    final persona = switch (level) {
      SurvivalLevel.beginner =>
        'Easy real-talk words. Normal pace. Keep it simple. Intent-first.',
      SurvivalLevel.intermediate =>
        'Everyday natural speech with contractions. Normal pace. Include ONE everyday idiom naturally.',
      SurvivalLevel.advanced =>
        'Richer idioms and emotion. Faster pace feel. Longer natural sentences.',
      SurvivalLevel.hardcore =>
        'No constraints. Rapid-fire, heavy contractions, improvised persona. Extreme realism.',
    };
    final avoid = recentSituationHints
        .where((e) => e.trim().isNotEmpty)
        .take(20);
    final prompt =
        """
You are a content generator for a 3-second survival English roleplay game.

Theme: ${theme.label}
Level: $lvl
Level style: $persona

Generate $count DIFFERENT scenario items for this theme.

Self-correction (mandatory):
- After drafting the scenarios, re-read EACH sentence and fix grammar, unnatural phrasing, and awkward word choice.
- Remove any inappropriate content (hate/harassment/sexual/minors/violence/self-harm) and rewrite into a safe everyday situation.
- Ensure there are no swear words or slurs.

Hard constraints:
- Each scenario MUST be a distinct micro-topic within the theme (avoid repetition).
- Do NOT repeat these recent situations (avoid similar phrasing/topics):
${avoid.isEmpty ? '(none)' : avoid.map((e) => '- $e').join('\n')}
- Scenarios must be realistic everyday situations.
- Return STRICT JSON only.

Output format: JSON array of objects.
Each object MUST have:
{
  "title_ko": "...",
  "npcRole": "...",
  "situation": "...",            // English situation description
  "situation_ko": "...",         // Korean situation description
  "question_en": "...",          // optional reference opener line in English (1 line)
  "keywords": ["...", "..."]    // 6-10 lowercase keywords
}
Return ONLY the JSON array.
""";

    final text = await _generateText(
      prompt,
      temperature: 0.9,
      maxOutputTokens: 1400,
      responseMimeType: 'application/json',
    );
    if (text == null || text.trim().isEmpty) return null;
    try {
      final decoded = jsonDecode(text);
      if (decoded is! List) return null;
      return decoded
          .whereType<dynamic>()
          .map((e) => Map<String, dynamic>.from(e as Map))
          .toList();
    } catch (_) {
      return null;
    }
  }

  Future<String?> translateToKorean(String english) async {
    if (geminiApiKey.trim().isEmpty) return null;
    final s = english.trim();
    if (s.isEmpty) return null;
    final prompt =
        """
Translate the following English sentence into natural Korean.
Return ONLY the Korean translation. No quotes. No extra text.

English:
$s
""";
    final text = await _generateText(prompt);
    if (text == null) return null;
    final out = text.trim();
    return out.isEmpty ? null : out;
  }

  Future<GeminiFailureFeedback?> generateFailureFeedback({
    required ThemeDef theme,
    required SurvivalLevel level,
    required Scenario scenario,
    required String npcLine,
    required String userAnswer,
    required int passThreshold,
    required String failureReasonHintKorean,
  }) async {
    if (geminiApiKey.trim().isEmpty) return null;
    final lvl = levelLabel(level);
    final hint = failureReasonHintKorean.trim();
    final prompt =
        """
You are a strict but helpful English coach for a 3-second survival English game.

Theme: ${theme.label}
Level: $lvl
NPC role: ${scenario.npcRole}
Situation: ${scenario.situation}

NPC line: $npcLine
User answer (ASR transcription): $userAnswer
Pass threshold (context score): $passThreshold

Failure hint (Korean): ${hint.isEmpty ? "(none)" : hint}

Task:
Generate a coaching set in JSON. Make it short and practical.

Output MUST be strict JSON:
{
  "ideal_answer": "...",
  "ideal_answer_ko": "...",
  "tip_ko": "...",
  "reason_ko": "..."
}
Return ONLY JSON.
""";
    final text = await _generateText(prompt);
    if (text == null || text.trim().isEmpty) return null;
    try {
      final Map<String, dynamic> data = jsonDecode(text);
      final ideal = (data['ideal_answer'] ?? '').toString().trim();
      final idealKo = (data['ideal_answer_ko'] ?? '').toString().trim();
      final tipKo = (data['tip_ko'] ?? '').toString().trim();
      final reasonKo = (data['reason_ko'] ?? '').toString().trim();
      if (ideal.isEmpty ||
          idealKo.isEmpty ||
          tipKo.isEmpty ||
          reasonKo.isEmpty) {
        return null;
      }
      return GeminiFailureFeedback(
        idealAnswer: ideal,
        idealAnswerKorean: idealKo,
        tipKorean: tipKo,
        reasonKorean: reasonKo,
      );
    } catch (_) {
      return null;
    }
  }

  Future<GeminiTurnJudgement?> judgeAndNext({
    required ThemeDef theme,
    required SurvivalLevel level,
    required int passThreshold,
    required Scenario scenario,
    required List<Map<String, String>> history,
    required String npcLine,
    required String userAnswer,
  }) async {
    if (geminiApiKey.trim().isEmpty) return null;

    final lvl = levelLabel(level);
    final persona = switch (level) {
      SurvivalLevel.beginner =>
        'Level 1 (초급 - Beginner): "You are a very kind, patient native speaker. 유저가 단어 하나만 짧게 말해도 의도를 100% 긍정적으로 유추해서 친절하게 통과시키고, 대화를 이어갈 수 있는 아주 쉬운 질문을 던져라."',
      SurvivalLevel.intermediate =>
        'Level 2 (중급 - Intermediate): "You are a standard clerk/native speaker. 유저가 너무 짧게 답하면 통과는 시켜주되, \'What size?\', \'Anything else?\'처럼 구체적인 정보를 한 가지 더 물어봐라."',
      SurvivalLevel.advanced =>
        'Level 3 (고급 - Advanced): "You are a busy native speaker. 대화 중간에 반드시 \'돌발 상황(Unexpected variables)\'을 하나 던져라. (예: 메뉴 품절, 자리 없음, 카드 결제기 고장 등) 유저가 문장으로 상황을 모면하도록 유도해라."',
      SurvivalLevel.hardcore =>
        'Level 4 (최상급 - Master): "You are a very rushed, impatient native speaker in a bustling environment. 말을 빠르고 압박하듯이 하고(Time pressure), 원어민들이 쓰는 구어체나 슬랭을 섞어 써라. 유저가 빨리 결정하지 않으면 곤란해하는 티를 내라."',
    };

    final l4PressureRule = level == SurvivalLevel.hardcore
        ? '- MASTER HARD RULE: npc_next MUST include at least ONE explicit time-pressure/impatience word: "hurry" or "quick" or "busy" or "slammed" or "next in line".'
        : '';
    final l4JsonStrict = level == SurvivalLevel.hardcore
        ? 'MASTER JSON RULE (non-negotiable): Even if you roleplay as impatient, you MUST output a complete and valid JSON object ONLY. Include ALL required keys. Do not output empty strings for npc_next/ideal_answer.'
        : '';

    final prompt =
        """
You are the sole judge and dialogue engine for a 3-second survival English game.

Theme: ${theme.label}
Level: $lvl
NPC role: ${scenario.npcRole}
Situation: ${scenario.situation}

Conversation so far (most recent last):
${history.map((e) => "${e['role']}: ${e['text']}").join("\n")}

NPC line (current): $npcLine
User answer (ASR transcription): $userAnswer

Core policy (SURVIVALISM):
- If the meaning/intention is understandable in context, survival should be granted even with broken grammar or awkward phrasing.
- Only treat as failure when intent is clearly unrelated OR meaning is severely distorted by misunderstanding.

Level policy:
$persona

Scoring rule:
- Return context_score 0-100.
- Set relevant=true ONLY if context_score >= $passThreshold.
- IMPORTANT for Beginner: If intent is understandable, you MUST set context_score >= $passThreshold.

Task:
1) Decide survival (relevant).
2) Generate the next NPC line as a natural tail question or response that continues the conversation.
3) Provide coaching set (ideal answer + Korean meaning + one-line Korean tip).
4) Provide ONE-LINE Korean reason.

$l4JsonStrict

NPC_NEXT rules (very important):
- npc_next MUST directly react to the user's answer and the immediate context.
- npc_next MUST include at least ONE key word/requirement from the user's answer (e.g., menu name like "coffee", or a request like "no sugar").
- Reuse the user's key word(s) verbatim (case-insensitive match). Do NOT replace with a synonym.
- Do NOT ask a generic random question. Do NOT reset the topic.
- Prefer a short, natural follow-up question (1 sentence) that a real person would say next.
- If the user answered with a preference/choice, ask a specific next-step question based on it.
- If the user complained/asked for help, respond with a realistic service reply and ask one clarifying question.
- Avoid repeating the same wording as the previous NPC line.

$l4PressureRule

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

    Future<GeminiTurnJudgement?> parseTurn(String raw) async {
      final data = _parseJsonObject(raw);
      if (data == null) return null;
      final score = int.tryParse('${data['context_score']}') ?? 0;
      final relevant =
          (data['relevant'] == true) ||
          (data['relevant']?.toString().toLowerCase() == 'true');
      final reason = (data['reason_ko'] ?? '').toString().trim();
      final npcNext = (data['npc_next'] ?? '').toString().trim();
      final ideal = (data['ideal_answer'] ?? '').toString().trim();
      final idealKo = (data['ideal_answer_ko'] ?? '').toString().trim();
      final tip = (data['tip_ko'] ?? '').toString().trim();
      if (npcNext.isEmpty) return null;
      if (reason.isEmpty || ideal.isEmpty || idealKo.isEmpty) return null;
      return GeminiTurnJudgement(
        contextScore: score.clamp(0, 100),
        relevant: relevant,
        reasonKorean: reason,
        npcNextLine: npcNext,
        idealAnswer: ideal,
        idealAnswerKorean: idealKo,
        tipKorean: tip,
      );
    }

    final text = await _generateText(
      prompt,
      temperature: 0.7,
      maxOutputTokens: 520,
      responseMimeType: 'application/json',
    );
    if (text == null || text.trim().isEmpty) return null;
    final first = await parseTurn(text);
    if (first != null) return first;

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
Do not include any other text.

Previous output:
$text
""";

    final repaired = await _generateText(
      repairPrompt,
      temperature: 0.2,
      maxOutputTokens: 520,
      responseMimeType: 'application/json',
    );
    if (repaired == null || repaired.trim().isEmpty) return null;
    return parseTurn(repaired);
  }

  String _openingPrompt({
    required ThemeDef theme,
    required SurvivalLevel level,
    required Scenario scenario,
  }) {
    final lvl = levelLabel(level);
    final sourceKo = scenario.situationKo.trim();
    final sourceEn = scenario.situation.trim();
    final example = scenario.questionEn.trim();
    final leagueRule = switch (level) {
      SurvivalLevel.beginner =>
        'Beginner(초급): Real-Talk but easy words. Normal pace. Short, clear intent. 1 sentence. Avoid textbook phrases.',
      SurvivalLevel.intermediate =>
        'Intermediate(중급): Real-Talk with natural contractions/linking (wanna/gonna/kinda). Normal pace. Mix in ONE everyday idiom naturally.',
      SurvivalLevel.advanced =>
        'Advanced(고급): Rich idioms + emotion. Faster pace feel. Longer sentence (can be 2 short clauses). Natural nuance/politeness as needed.',
      SurvivalLevel.hardcore =>
        'Hardcore(최상급): No constraints. Very real, rapid-fire vibe, heavy contractions/linking, improvised persona. Must sound native-level. Aim for "100-point" realism.',
    };
    return """
You are an English-speaking NPC in a realistic roleplay game.

Theme: ${theme.label}
Level: $lvl
Your role: ${scenario.npcRole}

You will be given a situation source (Korean and/or English). Use it as the scenario context.
You MUST generate a NEW opening line every time. Do NOT copy any example text.

Global rule (Real-Talk):
- Use natural native phrasing used in real life (no robotic textbook lines).
- Maintain appropriate formality for the situation (e.g., hotel/front desk/interview: polite & professional; cafe/airport: natural service tone).

League rule:
$leagueRule

Situation source (Korean): ${sourceKo.isEmpty ? '(none)' : sourceKo}
Situation source (English): ${sourceEn.isEmpty ? '(none)' : sourceEn}
Reference example (do NOT copy): ${example.isEmpty ? '(none)' : example}

Task:
- Write ONE short, natural opening line in English that forces the user to respond immediately.
- Vary phrasing and sentence structure across runs to prevent memorization.
- Match difficulty/tone to the level.

Output: ONLY the English line. No quotes. No extra text.
""";
  }

  Future<String?> _generateText(
    String prompt, {
    double temperature = 0.4,
    int maxOutputTokens = 256,
    String? responseMimeType,
  }) async {
    lastHttpStatus = null;
    lastHttpBody = null;
    lastException = null;

    final now = DateTime.now();
    final cd = _cooldownUntil;
    if (cd != null && now.isBefore(cd)) {
      lastHttpStatus = 429;
      lastHttpBody = 'cooldown_until=${cd.toIso8601String()}';
      debugPrint('[Gemini_HTTP] cooldown active until=$cd');
      return null;
    }
    try {
      final body = {
        "contents": [
          {
            "role": "user",
            "parts": [
              {"text": prompt},
            ],
          },
        ],
        "generationConfig": {
          "temperature": temperature,
          "maxOutputTokens": maxOutputTokens,
          if (responseMimeType != null && responseMimeType.trim().isNotEmpty)
            "responseMimeType": responseMimeType.trim(),
        },
      };

      const apiVersions = <String>['v1beta', 'v1'];

      bool shouldRetryStatus(int status) {
        if (status == 408) return true;
        if (status >= 500 && status <= 599) return true;
        return false;
      }

      for (final api in apiVersions) {
        final triedThisApi = <String>{};
        final modelsToTry = _candidateModels();
        for (final model in modelsToTry) {
          if (triedThisApi.contains(model)) continue;
          triedThisApi.add(model);
          final uri = Uri.parse(
            'https://generativelanguage.googleapis.com/$api/models/$model:generateContent?key=$geminiApiKey',
          );

          http.Response? res;
          for (int attempt = 0; attempt < 3; attempt++) {
            try {
              res = await http
                  .post(
                    uri,
                    headers: {"Content-Type": "application/json"},
                    body: jsonEncode(body),
                  )
                  .timeout(const Duration(seconds: 22));
            } catch (e) {
              lastException = e.toString();
              debugPrint(
                '[Gemini_HTTP] exception api=$api model=$model attempt=$attempt err=$e',
              );
              if (attempt < 2) {
                await Future.delayed(
                  Duration(milliseconds: 300 * (attempt + 1) * (attempt + 1)),
                );
                continue;
              }
              return null;
            }

            if (res.statusCode == 429) {
              final raw = res.body;
              final clipped = raw.length > 800 ? raw.substring(0, 800) : raw;
              lastHttpStatus = 429;
              lastHttpBody = clipped;
              _cooldownUntil = DateTime.now().add(_cooldownOn429);
              debugPrint(
                '[Gemini_HTTP] api=$api model=$model status=429 body="$clipped"',
              );
              debugPrint(
                '[Gemini_HTTP] set cooldown until=$_cooldownUntil (status=429)',
              );
              return null;
            }

            if (res.statusCode == 404) {
              // Key/project may not have this model. Try discovering a supported model via listModels.
              if ((_resolvedModel ?? '').trim().isEmpty ||
                  _resolvedModel == model) {
                final resolved = await _resolveModelByListModels(api);
                if (resolved != null && resolved.trim().isNotEmpty) {
                  // Retry immediately with the newly resolved model.
                  if (!triedThisApi.contains(resolved)) {
                    triedThisApi.add(resolved);
                    final retryUri = Uri.parse(
                      'https://generativelanguage.googleapis.com/$api/models/$resolved:generateContent?key=$geminiApiKey',
                    );
                    try {
                      res = await http
                          .post(
                            retryUri,
                            headers: {"Content-Type": "application/json"},
                            body: jsonEncode(body),
                          )
                          .timeout(const Duration(seconds: 22));
                    } catch (e) {
                      lastException = e.toString();
                      return null;
                    }
                    // Let normal flow handle the retry response.
                    continue;
                  }
                }
              }
              break;
            }
            if (shouldRetryStatus(res.statusCode) && attempt < 2) {
              await Future.delayed(
                Duration(milliseconds: 250 * (attempt + 1) * (attempt + 1)),
              );
              continue;
            }
            break;
          }

          if (res == null) return null;

          if (res.statusCode < 200 || res.statusCode >= 300) {
            final raw = res.body;
            final clipped = raw.length > 800 ? raw.substring(0, 800) : raw;
            lastHttpStatus = res.statusCode;
            lastHttpBody = clipped;
            debugPrint(
              '[Gemini_HTTP] api=$api model=$model status=${res.statusCode} body="$clipped"',
            );

            if (res.statusCode == 429) {
              _cooldownUntil = DateTime.now().add(_cooldownOn429);
              debugPrint(
                '[Gemini_HTTP] set cooldown until=$_cooldownUntil (status=429)',
              );
            }

            if (res.statusCode == 404) {
              continue;
            }
            return null;
          }

          Map<String, dynamic> json;
          try {
            json = jsonDecode(res.body) as Map<String, dynamic>;
          } catch (e) {
            lastException = 'parse_error: $e';
            lastHttpStatus = res.statusCode;
            lastHttpBody = _clipBodyForDebug(res.body);
            return null;
          }

          final candidates = (json['candidates'] as List?) ?? [];
          if (candidates.isEmpty) {
            lastException = 'empty_candidates';
            lastHttpStatus = res.statusCode;
            lastHttpBody = _clipBodyForDebug(res.body);
            return null;
          }

          final content =
              (candidates.first as Map<String, dynamic>)['content']
                  as Map<String, dynamic>?;
          final parts = (content?['parts'] as List?) ?? [];
          if (parts.isEmpty) {
            lastException = 'empty_parts';
            lastHttpStatus = res.statusCode;
            lastHttpBody = _clipBodyForDebug(res.body);
            return null;
          }
          final text = (parts.first as Map<String, dynamic>)['text']
              ?.toString();
          if (text == null || text.trim().isEmpty) {
            lastException = 'empty_text';
            lastHttpStatus = res.statusCode;
            lastHttpBody = _clipBodyForDebug(res.body);
            return null;
          }
          lastHttpStatus = res.statusCode;
          lastHttpBody = _clipBodyForDebug(res.body);
          debugPrint('[Gemini_HTTP] api_selected=$api model_selected=$model');
          return text;
        }
      }

      return null;
    } catch (e) {
      lastException = e.toString();
      debugPrint('[Gemini_HTTP] exception err=$e');
      return null;
    }
  }

  String _clipBodyForDebug(String raw) {
    final clipped = raw.length > 800 ? raw.substring(0, 800) : raw;
    return clipped;
  }
}

Scenario _pickRandomScenario(
  List<Scenario> pool, {
  String? excludeId,
  Set<String>? avoidIds,
  int maxAttempts = 14,
}) {
  if (pool.isEmpty) {
    return const Scenario(
      id: 'fallback',
      title: 'Fallback',
      npcRole: 'staff',
      situation: 'Fallback situation.',
    );
  }

  final avoid = <String>{
    if (excludeId != null && excludeId.trim().isNotEmpty) excludeId,
    ...?avoidIds,
  };

  List<Scenario> candidateList() {
    if (avoid.isEmpty) return pool;
    final filtered = pool.where((s) => !avoid.contains(s.id)).toList();
    return filtered.isNotEmpty ? filtered : pool;
  }

  Scenario pickFrom(List<Scenario> list) {
    final idx = Random().nextInt(list.length);
    return list[idx];
  }

  Scenario chosen = pickFrom(candidateList());
  int attempt = 0;
  while (attempt < maxAttempts && avoid.contains(chosen.id)) {
    attempt++;
    chosen = pickFrom(candidateList());
  }

  final filteredCount = pool.where((s) => !avoid.contains(s.id)).length;
  debugPrint(
    "[scenario_pick] pool=${pool.length} avoid=${avoid.length} filtered=$filteredCount attempts=$attempt chosen=${chosen.id}",
  );
  return chosen;
}

List<Scenario> fallbackScenariosForTheme(String themeId) {
  switch (themeId) {
    case 'airport':
      return const [
        Scenario(
          id: "airport_lost_baggage",
          title: "수하물 분실 항의",
          npcRole: "baggage service staff",
          situation:
              "Your baggage didn't arrive. You must get help, ask for next steps, and push for a solution.",
          keywords: ["baggage", "bag", "lost", "missing", "claim", "help"],
        ),
        Scenario(
          id: "airport_immigration",
          title: "입국심사 압박",
          npcRole: "immigration officer",
          situation:
              "You are questioned at immigration. You must answer clearly and confidently.",
          keywords: ["visit", "purpose", "tourism", "business", "passport"],
        ),
      ];
    case 'cafe':
      return const [
        Scenario(
          id: "cafe_wrong_order",
          title: "주문 실수 정정",
          npcRole: "barista",
          situation:
              "Your drink is wrong. You must explain what's wrong and request a fix.",
          keywords: ["coffee", "order", "wrong", "cold", "hot"],
        ),
        Scenario(
          id: "cafe_refund",
          title: "환불/보상 요구",
          npcRole: "cafe manager",
          situation:
              "You're unhappy with the drink and want compensation. Negotiate firmly.",
          keywords: ["refund", "coffee", "order", "wrong", "money"],
        ),
      ];
    case 'workplace':
      return const [
        Scenario(
          id: "work_deadline",
          title: "마감 압박 대응",
          npcRole: "team lead",
          situation:
              "Your lead demands an update. You must explain status, risks, and ask for support.",
          keywords: ["status", "deadline", "risk", "blocker", "update"],
        ),
        Scenario(
          id: "work_conflict",
          title: "갈등 조정",
          npcRole: "coworker",
          situation:
              "A coworker blames you. You must defend your position and propose a resolution.",
          keywords: ["issue", "resolve", "misunderstanding", "agree", "plan"],
        ),
      ];
    case 'neighborhood':
    default:
      return const [
        Scenario(
          id: "neighbor_noise",
          title: "소음 항의",
          npcRole: "neighbor",
          situation:
              "There is a noise issue. You must complain politely but clearly and reach agreement.",
          keywords: ["noise", "quiet", "late", "please", "sorry"],
        ),
        Scenario(
          id: "neighbor_package",
          title: "택배 분실/오배송",
          npcRole: "delivery person",
          situation:
              "Your package is missing. You must ask questions and request proof or follow-up.",
          keywords: ["package", "delivery", "missing", "tracking", "proof"],
        ),
      ];
  }
}

class AppBootstrapPage extends StatefulWidget {
  const AppBootstrapPage({super.key});

  @override
  State<AppBootstrapPage> createState() => _AppBootstrapPageState();
}

class _AppBootstrapPageState extends State<AppBootstrapPage> {
  @override
  void initState() {
    super.initState();
    unawaited(contentStore.initAndRefresh());
    unawaited(appConfigStore.initAndRefresh());
  }

  @override
  Widget build(BuildContext context) {
    return AnimatedBuilder(
      animation: contentStore,
      builder: (context, _) {
        if (contentStore.loading && contentStore.content == null) {
          return const Scaffold(
            backgroundColor: Color(0xFF0A0A0A),
            body: Center(
              child: CircularProgressIndicator(color: Colors.cyanAccent),
            ),
          );
        }

        if (contentStore.error != null && contentStore.content == null) {
          return Scaffold(
            backgroundColor: const Color(0xFF0A0A0A),
            body: Center(
              child: Padding(
                padding: const EdgeInsets.all(24),
                child: Column(
                  mainAxisAlignment: MainAxisAlignment.center,
                  children: [
                    Text(
                      appConfigStore.t('error.content_load_title'),
                      style: const TextStyle(
                        fontSize: 20,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                    const SizedBox(height: 12),
                    Text(
                      contentStore.error ?? "",
                      textAlign: TextAlign.center,
                      style: TextStyle(
                        color: Colors.white.withValues(alpha: 0.7),
                      ),
                    ),
                    const SizedBox(height: 16),
                    ElevatedButton(
                      onPressed: () => unawaited(contentStore.initAndRefresh()),
                      child: Text(appConfigStore.t('common.retry')),
                    ),
                  ],
                ),
              ),
            ),
          );
        }

        return const LobbyPage();
      },
    );
  }
}

class TitlePage extends StatelessWidget {
  const TitlePage({super.key});

  @override
  Widget build(BuildContext context) {
    final rawTitle = appConfigStore.t('app.title');
    final title = (rawTitle == 'app.title') ? '3초 생존 영어' : rawTitle;
    return Scaffold(
      backgroundColor: const Color(0xFF0A0A0A),
      body: SafeArea(
        child: Padding(
          padding: const EdgeInsets.all(24),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.stretch,
            children: [
              const Spacer(),
              Text(
                title,
                textAlign: TextAlign.center,
                style: const TextStyle(
                  fontSize: 34,
                  fontWeight: FontWeight.w900,
                  letterSpacing: -0.5,
                ),
              ),
              const SizedBox(height: 14),
              Text(
                '3초 안에 말하면 생존. 늦으면 탈락.',
                textAlign: TextAlign.center,
                style: TextStyle(
                  fontSize: 16,
                  color: Colors.white.withValues(alpha: 0.75),
                  fontWeight: FontWeight.w600,
                ),
              ),
              const SizedBox(height: 32),
              Container(
                padding: const EdgeInsets.all(18),
                decoration: BoxDecoration(
                  color: Colors.white.withValues(alpha: 0.06),
                  borderRadius: BorderRadius.circular(18),
                  border: Border.all(
                    color: Colors.white.withValues(alpha: 0.12),
                    width: 1,
                  ),
                ),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.stretch,
                  children: [
                    const Text(
                      '게임 룰',
                      style: TextStyle(
                        fontSize: 18,
                        fontWeight: FontWeight.w800,
                      ),
                    ),
                    const SizedBox(height: 10),
                    Text(
                      'NPC 질문이 끝나면 3초 안에 탭해서 말해.\n성공하면 대화가 계속 이어지고, 실패하면 코칭 + 부활(광고) 또는 처음부터.',
                      style: TextStyle(
                        fontSize: 14,
                        height: 1.4,
                        color: Colors.white.withValues(alpha: 0.80),
                        fontWeight: FontWeight.w600,
                      ),
                    ),
                  ],
                ),
              ),
              const SizedBox(height: 24),
              ElevatedButton(
                onPressed: () {
                  logEventSafe('title_start_click');
                  Navigator.of(context).pushReplacement(
                    MaterialPageRoute(builder: (_) => const AppBootstrapPage()),
                  );
                },
                style: ElevatedButton.styleFrom(
                  backgroundColor: Colors.cyanAccent,
                  foregroundColor: Colors.black,
                  padding: const EdgeInsets.symmetric(
                    horizontal: 18,
                    vertical: 16,
                  ),
                ),
                child: const Text(
                  '시작하기',
                  style: TextStyle(fontSize: 18, fontWeight: FontWeight.w900),
                ),
              ),
              const Spacer(),
              Text(
                kDebugMode ? _debugBuildTag : '',
                textAlign: TextAlign.center,
                style: TextStyle(
                  fontSize: 12,
                  color: Colors.white.withValues(alpha: 0.35),
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }
}

class LobbyPage extends StatefulWidget {
  const LobbyPage({super.key});

  @override
  State<LobbyPage> createState() => _LobbyPageState();
}

class _LobbyPageState extends State<LobbyPage> {
  BannerAd? _banner;
  bool _bannerReady = false;

  void _openLeaderboard() {
    Navigator.of(
      context,
    ).push(MaterialPageRoute(builder: (_) => const LeaderboardPage()));
  }

  String _bannerAdUnitId() {
    if (kIsWeb) return '';
    if (Platform.isAndroid) return 'ca-app-pub-3940256099942544/6300978111';
    if (Platform.isIOS) return 'ca-app-pub-3940256099942544/2934735716';
    return '';
  }

  @override
  void initState() {
    super.initState();
    final unitId = _bannerAdUnitId();
    if (unitId.trim().isEmpty) return;
    final ad = BannerAd(
      adUnitId: unitId,
      size: AdSize.banner,
      request: const AdRequest(),
      listener: BannerAdListener(
        onAdLoaded: (_) {
          if (!mounted) return;
          setState(() => _bannerReady = true);
          logEventSafe('banner_loaded', parameters: {'screen': 'lobby'});
        },
        onAdFailedToLoad: (ad, err) {
          debugPrint('[Ad_Banner][Lobby] failed $err');
          logEventSafe(
            'banner_failed',
            parameters: {'screen': 'lobby', 'err': err.toString()},
          );
          ad.dispose();
        },
      ),
    );
    _banner = ad;
    unawaited(ad.load());
  }

  @override
  void dispose() {
    _banner?.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final themes = contentStore.getThemes().isNotEmpty
        ? contentStore.getThemes()
        : const [
            ThemeDef(id: 'cafe', label: '카페', emoji: '☕️'),
            ThemeDef(id: 'airport', label: '공항', emoji: '🛫'),
            ThemeDef(id: 'hotel', label: '호텔', emoji: '🏨'),
            ThemeDef(id: 'clothing', label: '옷가게', emoji: '🛍️'),
            ThemeDef(id: 'workplace', label: '사회생활', emoji: '💼'),
            ThemeDef(id: 'neighborhood', label: '이웃', emoji: '🏘️'),
            ThemeDef(id: 'directions', label: '길찾기', emoji: '📍'),
            ThemeDef(id: 'interview', label: '면접', emoji: '🎤'),
            ThemeDef(id: 'party', label: '파티', emoji: '🥳'),
            ThemeDef(id: 'hospital', label: '병원', emoji: '🏥'),
          ];
    return Scaffold(
      backgroundColor: const Color(0xFF0A0A0A),
      appBar: AppBar(
        title: Text(
          kDebugMode
              ? '${(appConfigStore.t('app.title') == 'Survival English') ? '3초 생존 영어' : appConfigStore.t('app.title')} ($_debugBuildTag)'
              : (appConfigStore.t('app.title') == 'Survival English')
              ? '3초 생존 영어'
              : appConfigStore.t('app.title'),
        ),
        backgroundColor: Colors.transparent,
        actions: [
          IconButton(
            onPressed: _openLeaderboard,
            icon: const Icon(Icons.emoji_events_outlined),
          ),
        ],
      ),
      body: Padding(
        padding: const EdgeInsets.all(20),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            const SizedBox(height: 10),
            ElevatedButton.icon(
              onPressed: _openLeaderboard,
              icon: const Icon(Icons.emoji_events_outlined),
              label: const Text('랭킹 / 로그인'),
              style: ElevatedButton.styleFrom(
                backgroundColor: Colors.white.withValues(alpha: 0.08),
                foregroundColor: Colors.white,
                padding: const EdgeInsets.symmetric(vertical: 14),
                shape: RoundedRectangleBorder(
                  borderRadius: BorderRadius.circular(14),
                  side: BorderSide(color: Colors.white.withValues(alpha: 0.12)),
                ),
              ),
            ),
            const SizedBox(height: 16),
            Text(
              appConfigStore.t('lobby.select_theme'),
              style: const TextStyle(fontSize: 22, fontWeight: FontWeight.bold),
            ),
            const SizedBox(height: 16),
            Expanded(
              child: GridView.builder(
                itemCount: themes.length,
                gridDelegate: const SliverGridDelegateWithFixedCrossAxisCount(
                  crossAxisCount: 2,
                  crossAxisSpacing: 12,
                  mainAxisSpacing: 12,
                  childAspectRatio: 1.25,
                ),
                itemBuilder: (context, index) {
                  final theme = themes[index];
                  return InkWell(
                    onTap: () {
                      logEventSafe(
                        'select_theme',
                        parameters: {'theme_id': theme.id},
                      );
                      Navigator.of(context).push(
                        MaterialPageRoute(
                          builder: (_) => ThemeLevelPage(theme: theme),
                        ),
                      );
                    },
                    borderRadius: BorderRadius.circular(16),
                    child: Container(
                      decoration: BoxDecoration(
                        borderRadius: BorderRadius.circular(16),
                        color: Colors.white.withValues(alpha: 0.06),
                        border: Border.all(
                          color: Colors.white.withValues(alpha: 0.08),
                        ),
                      ),
                      padding: const EdgeInsets.all(16),
                      child: Column(
                        mainAxisAlignment: MainAxisAlignment.center,
                        children: [
                          Text(
                            theme.emoji,
                            style: const TextStyle(fontSize: 40),
                          ),
                          const SizedBox(height: 10),
                          Text(
                            theme.label,
                            style: const TextStyle(
                              fontSize: 20,
                              fontWeight: FontWeight.w800,
                            ),
                          ),
                        ],
                      ),
                    ),
                  );
                },
              ),
            ),
          ],
        ),
      ),
      floatingActionButton:
          kDebugMode && bool.fromEnvironment('SHOW_SEED', defaultValue: false)
          ? DebugSeedButton()
          : null,
      bottomNavigationBar: _bannerReady && _banner != null
          ? SizedBox(
              height: _banner!.size.height.toDouble(),
              width: double.infinity,
              child: AdWidget(ad: _banner!),
            )
          : null,
    );
  }
}

class DebugSeedButton extends StatefulWidget {
  const DebugSeedButton({super.key});

  @override
  State<DebugSeedButton> createState() => _DebugSeedButtonState();
}

class _DebugSeedButtonState extends State<DebugSeedButton> {
  bool _seeding = false;

  Future<void> _runSeedingFlow() async {
    if (!kDebugMode) return;
    if (_seeding) return;

    bool overwrite = false;
    final confirmed = await showDialog<bool>(
      context: context,
      barrierDismissible: false,
      builder: (ctx) {
        return StatefulBuilder(
          builder: (ctx, setLocal) {
            return AlertDialog(
              title: Text(appConfigStore.t('seed.title')),
              content: Column(
                mainAxisSize: MainAxisSize.min,
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(appConfigStore.t('seed.description')),
                  const SizedBox(height: 12),
                  Row(
                    children: [
                      Checkbox(
                        value: overwrite,
                        onChanged: (v) =>
                            setLocal(() => overwrite = (v ?? false)),
                      ),
                      Expanded(child: Text(appConfigStore.t('seed.overwrite'))),
                    ],
                  ),
                ],
              ),
              actions: [
                TextButton(
                  onPressed: () => Navigator.of(ctx).pop(false),
                  child: Text(appConfigStore.t('common.cancel')),
                ),
                ElevatedButton(
                  onPressed: () => Navigator.of(ctx).pop(true),
                  child: Text(appConfigStore.t('common.upload')),
                ),
              ],
            );
          },
        );
      },
    );

    if (confirmed != true) return;

    setState(() => _seeding = true);
    if (!mounted) return;

    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text(appConfigStore.t('seed.in_progress')),
        duration: const Duration(seconds: 2),
      ),
    );

    try {
      final result = await _seedInitialContent(overwrite: overwrite);
      await contentStore.initAndRefresh();
      await appConfigStore.initAndRefresh();
      if (!mounted) return;
      await showDialog<void>(
        context: context,
        builder: (ctx) {
          return AlertDialog(
            title: Text(appConfigStore.t('seed.done')),
            content: Text(
              "themes_written=${result.themesWritten}\nscenarios_written=${result.scenariosWritten}\nskipped=${result.skipped}",
            ),
            actions: [
              TextButton(
                onPressed: () => Navigator.of(ctx).pop(),
                child: Text(appConfigStore.t('common.ok')),
              ),
            ],
          );
        },
      );
    } catch (e) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text("${appConfigStore.t('seed.failed')}: $e")),
      );
    } finally {
      if (mounted) setState(() => _seeding = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    const showSeed = bool.fromEnvironment('SHOW_SEED', defaultValue: false);
    if (!kDebugMode || !showSeed) return const SizedBox.shrink();
    return FloatingActionButton.extended(
      onPressed: _seeding ? null : _runSeedingFlow,
      backgroundColor: Colors.purpleAccent,
      label: Text(
        _seeding
            ? appConfigStore.t('seed.button_busy')
            : appConfigStore.t('seed.button'),
      ),
      icon: const Icon(Icons.cloud_upload),
    );
  }
}

class _SeedResult {
  final int themesWritten;
  final int scenariosWritten;
  final int skipped;
  const _SeedResult({
    required this.themesWritten,
    required this.scenariosWritten,
    required this.skipped,
  });
}

Future<_SeedResult> _seedInitialContent({required bool overwrite}) async {
  final firestore = FirebaseFirestore.instance;

  final themes = const <ThemeDef>[
    ThemeDef(id: 'cafe', label: '카페', emoji: '☕️'),
    ThemeDef(id: 'airport', label: '공항', emoji: '🛫'),
    ThemeDef(id: 'hotel', label: '호텔', emoji: '🏨'),
    ThemeDef(id: 'clothing', label: '옷가게', emoji: '🛍️'),
    ThemeDef(id: 'workplace', label: '사회생활', emoji: '💼'),
    ThemeDef(id: 'neighborhood', label: '이웃', emoji: '🏘️'),
    ThemeDef(id: 'directions', label: '길찾기', emoji: '📍'),
    ThemeDef(id: 'interview', label: '면접', emoji: '🎤'),
    ThemeDef(id: 'party', label: '파티', emoji: '🥳'),
    ThemeDef(id: 'hospital', label: '병원', emoji: '🏥'),
  ];

  final themeKeywords = {
    'cafe': const ['coffee', 'order', 'wrong', 'hot', 'cold'],
    'airport': const ['passport', 'baggage', 'lost', 'gate', 'flight'],
    'hotel': const ['check-in', 'reservation', 'room', 'key', 'late'],
    'clothing': const ['size', 'try', 'refund', 'exchange', 'color'],
    'workplace': const ['deadline', 'update', 'meeting', 'issue', 'help'],
    'neighborhood': const ['noise', 'package', 'parking', 'sorry', 'please'],
    'directions': const ['where', 'turn', 'left', 'right', 'near'],
    'interview': const ['experience', 'strength', 'weakness', 'salary', 'role'],
    'party': const ['nice', 'meet', 'drink', 'music', 'friends'],
    'hospital': const ['pain', 'symptoms', 'medicine', 'appointment', 'doctor'],
  };

  final themeRoles = <String, String>{
    'cafe': 'barista',
    'airport': 'airport staff',
    'hotel': 'front desk staff',
    'clothing': 'store clerk',
    'workplace': 'coworker',
    'neighborhood': 'neighbor',
    'directions': 'local person',
    'interview': 'interviewer',
    'party': 'new friend',
    'hospital': 'doctor',
  };

  final themeSituations = <String, String>{
    'cafe': 'You need to order or fix a problem with your drink quickly.',
    'airport': 'You need help with travel procedures under time pressure.',
    'hotel': 'You need to solve a reservation/room issue politely but fast.',
    'clothing': 'You need the right size or need to exchange/refund an item.',
    'workplace': 'You must communicate status, blockers, and requests clearly.',
    'neighborhood':
        'You must handle a small conflict politely with a neighbor.',
    'directions': 'You are lost and need simple directions.',
    'interview':
        'You must answer an interview question clearly and confidently.',
    'party':
        'You are meeting someone new and must keep the conversation going.',
    'hospital': 'You must explain symptoms and ask for help.',
  };

  int themesWritten = 0;
  int scenariosWritten = 0;
  int skipped = 0;

  final batch = firestore.batch();

  Future<bool> docExists(DocumentReference ref) async {
    final snap = await ref.get();
    return snap.exists;
  }

  final configRef = firestore.collection('app_config').doc('current');
  final configExists = overwrite ? false : await docExists(configRef);
  if (!configExists || overwrite) {
    batch.set(configRef, {
      'strings': {
        'app.title': '3초 생존 영어',
        'lobby.select_theme': '테마 선택',
        'seed.title': '개발자용 시딩',
        'seed.description': 'Firestore에 테마 10개 + 레벨별 시나리오를 업로드합니다.',
        'seed.overwrite': '기존 데이터를 덮어쓰기(Overwrite)',
        'seed.in_progress': '시딩 시작: Firestore 업로드 중...',
        'seed.done': '시딩 완료',
        'seed.failed': '시딩 실패',
        'seed.button': 'DEV 시딩',
        'seed.button_busy': '시딩 중...',
        'common.cancel': '취소',
        'common.upload': '업로드',
        'common.ok': '확인',
        'common.retry': '다시 시도',
        'scenario.preparing_new': '새로운 상황을 준비 중입니다',
        'error.content_load_title': '컨텐츠 로딩이 잠깐 꼬였어요',
        'error.mic_permission_needed': '마이크 권한이 필요해요. 설정에서 마이크를 허용하고 다시 와주세요.',
        'error.record_start_failed': '녹음 시작에 실패했어요. 마이크/오디오 장치 상태를 한번만 확인해주세요.',
        'error.stt_init_failed': 'STT가 잠깐 삐끗했어요. 기기 음성 인식 권한/설정을 확인해볼까요?',
        'error.stt_listen_failed': '말을 듣다가 잠깐 끊겼어요. 다시 한 번만 말해볼까요?',
        'error.network_status': 'AI가 잠깐 졸고 있어요(네트워크). 잠시 후 다시 눌러주세요.',
        'error.network_with_code': 'AI가 잠깐 졸고 있어요(네트워크 {code}). 다시 말해볼까요?',
        'error.empty_speech': '앗, 아무 말도 안 들렸어요. 다시 한 번만 말해볼까요?',
        'error.ai_judge_failed': 'AI가 판정에 실패했어요. 네트워크 상태를 확인하고 다시 시도해 주세요.',
        'error.timeout_reason': '시간 내에 대답하지 못했어요.',
        'error.timeout_spoken': '⏰ Time Out! (대답 지연)',
        'error.pool_too_small': '새로운 상황을 준비 중입니다',
      },
      'prompts': {
        'gemini.opening': '',
        'gemini.judge_and_next': '',
        'gemini.translate_ko': '',
        'gemini.failure_feedback': '',
      },
      'updated_at': DateTime.now().toUtc().toIso8601String(),
    }, SetOptions(merge: overwrite));
  } else {
    skipped++;
  }

  for (final theme in themes) {
    final themeRef = firestore.collection('themes').doc(theme.id);
    final themeExists = overwrite ? false : await docExists(themeRef);
    if (!themeExists || overwrite) {
      batch.set(themeRef, {
        'label': theme.label,
        'label_ko': theme.label,
        'emoji': theme.emoji,
      }, SetOptions(merge: overwrite));
      themesWritten++;
    } else {
      skipped++;
    }

    for (final level in SurvivalLevel.values) {
      for (int i = 1; i <= 10; i++) {
        final scenarioId =
            "${theme.id}_${levelId(level)}_${i.toString().padLeft(2, '0')}";
        final scenarioRef = themeRef.collection('scenarios').doc(scenarioId);
        final scenarioExists = overwrite ? false : await docExists(scenarioRef);
        if (scenarioExists && !overwrite) {
          skipped++;
          continue;
        }

        final npcRole = themeRoles[theme.id] ?? 'staff';
        final situation =
            themeSituations[theme.id] ??
            'Handle a real-life situation quickly.';
        final keywords = themeKeywords[theme.id] ?? const <String>[];

        batch.set(scenarioRef, {
          'title': "${theme.label} 오프닝 ${i.toString().padLeft(2, '0')}",
          'title_ko': "${theme.label} 오프닝 ${i.toString().padLeft(2, '0')}",
          'question_en': '',
          'situation_ko': '',
          'npcRole': npcRole,
          'situation': situation,
          'keywords': keywords,
          'level': levelId(level),
        }, SetOptions(merge: overwrite));
        scenariosWritten++;
      }
    }
  }

  final realOpenings = <({String themeId, String tagKo, String questionEn, String situationKo})>[
    (
      themeId: 'cafe',
      tagKo: '품절',
      questionEn:
          "Sorry, we’re all out of oat milk. Would regular or soy be okay?",
      situationKo: '오트밀크가 다 떨어졌는데, 일반이나 두유도 괜찮으세요?',
    ),
    (
      themeId: 'cafe',
      tagKo: '재확인',
      questionEn: "Did you want that iced or hot? You didn't specify.",
      situationKo: '아이스예요, 핫이에요? 말씀을 안 하셔서요.',
    ),
    (
      themeId: 'cafe',
      tagKo: '이름 확인',
      questionEn: "Can I get a name for the order, please?",
      situationKo: '주문하신 분 성함 좀 알려주시겠어요?',
    ),
    (
      themeId: 'cafe',
      tagKo: '개인컵',
      questionEn: "Is that in a reusable cup or a paper cup?",
      situationKo: '텀블러에 드릴까요, 종이컵에 드릴까요?',
    ),
    (
      themeId: 'cafe',
      tagKo: '포장',
      questionEn: "Are you having that here, or is it to-go?",
      situationKo: '드시고 가세요, 가져가세요?',
    ),
    (
      themeId: 'cafe',
      tagKo: '진동벨',
      questionEn:
          "Here’s your buzzer. It’ll vibrate when your drink is ready, okay?",
      situationKo: '여기 진동벨요. 음료 나오면 울릴 거예요, 아셨죠?',
    ),
    (
      themeId: 'cafe',
      tagKo: '휘핑크림',
      questionEn: "Would you like whipped cream on top of your mocha?",
      situationKo: '모카 위에 휘핑크림 올려드릴까요?',
    ),
    (
      themeId: 'airport',
      tagKo: '수하물',
      questionEn:
          'Do you have any fragile items or liquids in your checked bag?',
      situationKo: '위탁 수하물 안에 깨지기 쉬운 물건이나 액체류 있나요?',
    ),
    (
      themeId: 'airport',
      tagKo: '경유지',
      questionEn:
          'Your flight has a layover in Tokyo. Do you need to recheck your bags?',
      situationKo: '도쿄 경유입니다. 짐 다시 부치셔야 하는 거 아시죠?',
    ),
    (
      themeId: 'airport',
      tagKo: '좌석 변경',
      questionEn:
          'We have an exit row seat available. Would you like to switch?',
      situationKo: '비상구 좌석이 비었는데, 옮기시겠어요?',
    ),
    (
      themeId: 'airport',
      tagKo: '기내식',
      questionEn: 'Would you like the beef or the vegetarian pasta?',
      situationKo: '소고기 드실래요, 채식 파스타 드실래요?',
    ),
    (
      themeId: 'airport',
      tagKo: '세관',
      questionEn: 'Do you have anything to declare to customs?',
      situationKo: '세관에 신고할 물건 있습니까?',
    ),
    (
      themeId: 'airport',
      tagKo: '입국 목적',
      questionEn:
          'What is the primary purpose of your visit to the United States?',
      situationKo: '미국 방문의 주요 목적이 뭡니까?',
    ),
    (
      themeId: 'hotel',
      tagKo: '디파짓',
      questionEn:
          'We need a credit card for a \$100 security deposit. Is that okay?',
      situationKo: '보증금 100달러 결제할 카드가 필요합니다. 괜찮으시죠?',
    ),
    (
      themeId: 'hotel',
      tagKo: '조식',
      questionEn:
          'Breakfast is served from 7 to 10 AM in the lobby. Will you be joining us?',
      situationKo: '조식은 7시부터 10시까지입니다. 이용하실 건가요?',
    ),
    (
      themeId: 'hotel',
      tagKo: '얼리 체크인',
      questionEn:
          "Your room isn't ready yet. Would you like us to hold your luggage?",
      situationKo: '방이 아직 안 됐네요. 짐 좀 맡아드릴까요?',
    ),
    (
      themeId: 'hotel',
      tagKo: '와이파이',
      questionEn:
          'The Wi-Fi password is on the back of your key sleeve. Do you see it?',
      situationKo: '와이파이 비번은 카드 키 홀더 뒤에 있어요. 보이시나요?',
    ),
    (
      themeId: 'hotel',
      tagKo: '체크아웃 시간',
      questionEn:
          'Just a reminder, check-out is at 11 AM sharp. Any questions?',
      situationKo: '체크아웃은 오전 11시 정각입니다. 궁금한 거 있으세요?',
    ),
    (
      themeId: 'hotel',
      tagKo: '금연실',
      questionEn:
          'All our rooms are non-smoking. There’s a fine for smoking inside, okay?',
      situationKo: '저희 객실은 모두 금연입니다. 실내 흡연 시 과태료 있어요, 아셨죠?',
    ),
    (
      themeId: 'hotel',
      tagKo: '추가 수건',
      questionEn:
          "You requested extra towels. I'll send them up in 10 minutes, alright?",
      situationKo: '수건 추가 요청하셨죠. 10분 뒤에 올려보낼게요, 됐나요?',
    ),
  ];

  for (int i = 0; i < realOpenings.length; i++) {
    final o = realOpenings[i];
    final seededThemeRef = firestore.collection('themes').doc(o.themeId);
    final seededScenarioId =
        "${o.themeId}_beginner_real_${(i + 1).toString().padLeft(2, '0')}";
    final seededScenarioRef = seededThemeRef
        .collection('scenarios')
        .doc(seededScenarioId);
    final seededScenarioExists = overwrite
        ? false
        : await docExists(seededScenarioRef);
    if (seededScenarioExists && !overwrite) {
      skipped++;
      continue;
    }

    final title = "[${o.tagKo}] ${o.questionEn}";
    final npcRole = themeRoles[o.themeId] ?? 'staff';
    final situation =
        themeSituations[o.themeId] ?? 'Handle a real-life situation quickly.';

    batch.set(seededScenarioRef, {
      'title': title,
      'title_ko': title,
      'question_en': o.questionEn,
      'situation_ko': o.situationKo,
      'npcRole': npcRole,
      'situation': situation,
      'keywords': themeKeywords[o.themeId] ?? const <String>[],
      'level': levelId(SurvivalLevel.beginner),
    }, SetOptions(merge: overwrite));
    scenariosWritten++;
  }

  await batch.commit();
  return _SeedResult(
    themesWritten: themesWritten,
    scenariosWritten: scenariosWritten,
    skipped: skipped,
  );
}

class ThemeLevelPage extends StatefulWidget {
  final ThemeDef theme;
  const ThemeLevelPage({super.key, required this.theme});

  @override
  State<ThemeLevelPage> createState() => _ThemeLevelPageState();
}

class _ThemeLevelPageState extends State<ThemeLevelPage> {
  BannerAd? _banner;
  bool _bannerReady = false;

  String _bannerAdUnitId() {
    if (kIsWeb) return '';
    if (Platform.isAndroid) return 'ca-app-pub-3940256099942544/6300978111';
    if (Platform.isIOS) return 'ca-app-pub-3940256099942544/2934735716';
    return '';
  }

  @override
  void initState() {
    super.initState();
    final unitId = _bannerAdUnitId();
    if (unitId.trim().isEmpty) return;
    final ad = BannerAd(
      adUnitId: unitId,
      size: AdSize.banner,
      request: const AdRequest(),
      listener: BannerAdListener(
        onAdLoaded: (_) {
          if (!mounted) return;
          setState(() => _bannerReady = true);
        },
        onAdFailedToLoad: (ad, err) {
          debugPrint('[Ad_Banner][Level] failed $err');
          ad.dispose();
        },
      ),
    );
    _banner = ad;
    unawaited(ad.load());
  }

  @override
  void dispose() {
    _banner?.dispose();
    super.dispose();
  }

  Future<void> _startLevel(SurvivalLevel level) async {
    final nav = Navigator.of(context);
    final rootNav = Navigator.of(context, rootNavigator: true);
    final messenger = ScaffoldMessenger.of(context);

    var pool = contentStore.getScenarios(widget.theme.id, level);

    bool dialogShown = false;
    if (pool.isEmpty && mounted) {
      dialogShown = true;
      showDialog<void>(
        context: context,
        barrierDismissible: false,
        builder: (_) {
          return const AlertDialog(
            content: Row(
              children: [
                SizedBox(
                  width: 22,
                  height: 22,
                  child: CircularProgressIndicator(),
                ),
                SizedBox(width: 12),
                Expanded(child: Text('새로운 상황을 준비 중입니다...')),
              ],
            ),
          );
        },
      );
    }

    if (pool.isEmpty) {
      unawaited(
        contentStore.ensureScenarioCache(
          themeId: widget.theme.id,
          level: level,
          minCount: 10,
        ),
      );
      final start = DateTime.now();
      while (DateTime.now().difference(start) < const Duration(seconds: 10)) {
        await Future.delayed(const Duration(milliseconds: 650));
        pool = contentStore.getScenarios(widget.theme.id, level);
        if (pool.isNotEmpty) break;
      }
    }

    if (!mounted) return;
    if (dialogShown) {
      rootNav.pop();
    }

    pool = contentStore.getScenarios(widget.theme.id, level);
    if (pool.isEmpty) {
      logEventSafe(
        'scenario_wait_timeout',
        parameters: {
          'theme_id': widget.theme.id,
          'level': levelId(level),
          'entry': 'level_select',
        },
      );
      messenger.showSnackBar(
        const SnackBar(content: Text('통신이 원활하지 않습니다. 로비로 돌아갑니다.')),
      );
      nav.popUntil((r) => r.isFirst);
      return;
    }

    final scenario = _pickRandomScenario(pool);
    logEventSafe(
      'game_start',
      parameters: {
        'theme_id': widget.theme.id,
        'level': levelId(level),
        'scenario_id': scenario.id,
        'entry': 'level_select',
      },
    );
    nav.push(
      MaterialPageRoute(
        builder: (_) =>
            GamePage(theme: widget.theme, level: level, scenario: scenario),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final levels = SurvivalLevel.values;
    return Scaffold(
      backgroundColor: const Color(0xFF0A0A0A),
      appBar: AppBar(
        title: Text("${widget.theme.emoji} ${widget.theme.label}"),
        backgroundColor: Colors.transparent,
      ),
      bottomNavigationBar: _bannerReady && _banner != null
          ? SizedBox(
              height: _banner!.size.height.toDouble(),
              width: double.infinity,
              child: AdWidget(key: ValueKey(_banner.hashCode), ad: _banner!),
            )
          : null,
      body: Padding(
        padding: const EdgeInsets.all(24),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            const SizedBox(height: 6),
            const Text(
              '레벨 선택',
              style: TextStyle(fontSize: 22, fontWeight: FontWeight.bold),
            ),
            const SizedBox(height: 16),
            Expanded(
              child: ListView.separated(
                itemCount: levels.length,
                separatorBuilder: (context, index) =>
                    const SizedBox(height: 12),
                itemBuilder: (context, idx) {
                  final level = levels[idx];
                  return InkWell(
                    onTap: () {
                      unawaited(_startLevel(level));
                    },
                    borderRadius: BorderRadius.circular(16),
                    child: Container(
                      decoration: BoxDecoration(
                        borderRadius: BorderRadius.circular(16),
                        color: Colors.white.withValues(alpha: 0.06),
                        border: Border.all(
                          color: Colors.white.withValues(alpha: 0.08),
                        ),
                      ),
                      padding: const EdgeInsets.all(16),
                      child: Row(
                        children: [
                          Container(
                            width: 44,
                            height: 44,
                            decoration: BoxDecoration(
                              color: Colors.cyan.withValues(alpha: 0.18),
                              borderRadius: BorderRadius.circular(12),
                            ),
                            alignment: Alignment.center,
                            child: Text(
                              levelLabel(level).substring(0, 1),
                              style: const TextStyle(
                                fontSize: 18,
                                fontWeight: FontWeight.bold,
                              ),
                            ),
                          ),
                          const SizedBox(width: 12),
                          Expanded(
                            child: Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                Text(
                                  levelLabel(level),
                                  style: const TextStyle(
                                    fontSize: 18,
                                    fontWeight: FontWeight.w900,
                                  ),
                                ),
                                const SizedBox(height: 4),
                                Text(
                                  '최대 ${maxRoundsForLevel(level) * 10}턴',
                                  style: TextStyle(
                                    color: Colors.white.withValues(alpha: 0.7),
                                  ),
                                ),
                              ],
                            ),
                          ),
                          const Icon(
                            Icons.chevron_right,
                            color: Colors.white54,
                          ),
                        ],
                      ),
                    ),
                  );
                },
              ),
            ),
          ],
        ),
      ),
    );
  }
}

enum GameState {
  ready,
  speakingQuestion,
  waitingForTap,
  recording,
  processing,
  result,
}

class GamePage extends StatefulWidget {
  final ThemeDef theme;
  final SurvivalLevel level;
  final Scenario scenario;
  const GamePage({
    super.key,
    required this.theme,
    required this.level,
    required this.scenario,
  });

  @override
  State<GamePage> createState() => _GamePageState();
}

class _GamePageState extends State<GamePage> {
  static const String _prefsDeathCountKey = 'death_count_v1';

  final FlutterTts _tts = FlutterTts();
  final AudioRecorder _audioRecorder = AudioRecorder();
  final AudioPlayer _audioPlayer = AudioPlayer();
  final stt.SpeechToText _speechToText = stt.SpeechToText();
  final AiClient _ai = FailoverAiClient(
    primary: GeminiAiClient(),
    secondary: OpenAiAiClient(),
  );

  DateTime? _lastAiErrorDialogAt;

  RewardedAd? _reviveRewardedAd;
  bool _reviveAdLoading = false;

  BannerAd? _banner;
  bool _bannerReady = false;

  InterstitialAd? _interstitial;
  bool _interstitialLoading = false;

  bool _isBadNpcLine(String line) {
    final s = line.trim();
    if (s.isEmpty) return true;
    final lower = s.toLowerCase();
    if (lower.length < 10) return true;
    if (lower.startsWith('okay, what') || lower == 'okay, what') return true;
    if (RegExp(r"\b(for|with|to|and|or|of)$").hasMatch(lower)) return true;
    if (RegExp(
      r"\b(can|could|would|should|may|might|will|shall|do|does|did)$",
    ).hasMatch(lower)) {
      return true;
    }
    if (!RegExp(r"[.?!]$").hasMatch(s)) return true;
    if (RegExp(
      r"\b(i|you|we|they|he|she|it|this|that|these|those|my|your|our|their|a|an|the)$",
    ).hasMatch(lower)) {
      return true;
    }
    if (RegExp(r"[,:]$").hasMatch(s)) return true;
    return false;
  }

  bool _isIdealAnswerCompatible({
    required String question,
    required String ideal,
  }) {
    final q = question.trim();
    final a = ideal.trim();
    if (q.isEmpty || a.isEmpty) return false;

    final qLower = q.toLowerCase();
    final isYesNoQuestion =
        qLower.startsWith('would you like') ||
        qLower.startsWith('do you want') ||
        qLower.contains('would you like ') ||
        qLower.contains('do you want ') ||
        qLower.contains('want that ') ||
        qLower.contains('want it ');

    const stop = <String>{
      'a',
      'an',
      'the',
      'i',
      'you',
      'we',
      'they',
      'he',
      'she',
      'it',
      'this',
      'that',
      'these',
      'those',
      'my',
      'your',
      'our',
      'their',
      'to',
      'for',
      'of',
      'and',
      'or',
      'in',
      'on',
      'at',
      'with',
      'is',
      'are',
      'am',
      'was',
      'were',
      'be',
      'been',
      'being',
      'do',
      'does',
      'did',
      'can',
      'could',
      'would',
      'should',
      'will',
      'shall',
      'may',
      'might',
      'what',
      'where',
      'when',
      'why',
      'how',
      'any',
      'anything',
    };

    final qTokens = _tokenSetFromEnglish(q)
      ..removeWhere((t) => stop.contains(t));
    final aTokens = _tokenSetFromEnglish(a)
      ..removeWhere((t) => stop.contains(t));

    if (isYesNoQuestion) {
      if (aTokens.contains('yes') || aTokens.contains('no')) return true;
    }

    final overlap = aTokens.where((t) => qTokens.contains(t)).length;
    return overlap >= 1;
  }

  bool _isGoodKoreanTranslation(String ko, String en) {
    final s = ko.trim();
    if (s.isEmpty) return false;
    if (RegExp(r"[,.]$").hasMatch(s)) return false;
    if (RegExp(r"(,|그리고|아니|근데)$").hasMatch(s)) return false;
    if (RegExp(r"[A-Za-z]").hasMatch(s)) return false;
    if (!RegExp(r"[가-힣]").hasMatch(s)) return false;
    if (RegExp(r"\([^)]+$").hasMatch(s)) return false;
    if (RegExp(r"\[[^\]]+$").hasMatch(s)) return false;
    if (RegExp(r"(…|\.\.\.)$").hasMatch(s)) return false;
    if (s.length < 6) return false;
    if (en.trim().length >= 18 && s.length < 8) return false;
    final enLen = en.trim().length;
    if (enLen >= 30 && s.length < 10) return false;
    if (RegExp(r"[.?!…]$").hasMatch(s)) return true;
    if (s.endsWith('요') ||
        s.endsWith('요?') ||
        s.endsWith('니다') ||
        s.endsWith('까요?')) {
      return true;
    }
    return false;
  }

  bool _isGoodKoreanSentence(String ko) {
    final s = ko.trim();
    if (s.isEmpty) return false;
    if (!RegExp(r"[가-힣]").hasMatch(s)) return false;
    if (RegExp(r"[A-Za-z]").hasMatch(s)) return false;
    if (s.length < 4) return false;
    if (RegExp(r"(…|\.\.\.)$").hasMatch(s)) return false;
    if (RegExp(r"(,|그리고|아니|근데)$").hasMatch(s)) return false;
    if (RegExp(r"[.?!]$").hasMatch(s)) return true;
    if (s.endsWith('요') ||
        s.endsWith('요?') ||
        s.endsWith('니다') ||
        s.endsWith('까요?') ||
        s.endsWith('해요') ||
        s.endsWith('해요?')) {
      return true;
    }
    return false;
  }

  void _showAiErrorDetails({required String title, String? fallback}) {
    if (!mounted) return;
    final client = _ai;
    final dbg = client is FailoverAiClient
        ? (client.lastDebugSummary?.trim() ?? '')
        : '';
    final msg = (dbg.isNotEmpty ? dbg : (fallback ?? '')).trim();
    if (msg.isEmpty) return;

    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text(msg.length > 160 ? msg.substring(0, 160) : msg),
        duration: const Duration(seconds: 4),
      ),
    );

    showDialog<void>(
      context: context,
      builder: (ctx) {
        return AlertDialog(
          title: Text(title),
          content: SingleChildScrollView(child: SelectableText(msg)),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(ctx).pop(),
              child: const Text('닫기'),
            ),
          ],
        );
      },
    );
  }

  void _showAiErrorDetailsThrottled({required String title, String? fallback}) {
    const enabled = bool.fromEnvironment(
      'SHOW_AI_MODAL_ERRORS',
      defaultValue: true,
    );
    if (!kDebugMode || !enabled) return;

    final now = DateTime.now();
    final last = _lastAiErrorDialogAt;
    if (last != null && now.difference(last) < const Duration(seconds: 5)) {
      return;
    }

    logEventSafe(
      'ai_error_modal',
      parameters: {
        'theme_id': widget.theme.id,
        'level': levelId(widget.level),
        'scenario_id': widget.scenario.id,
        'turn_in_round': _turnInRound,
        'round': _round,
        'title': title,
      },
    );
    _lastAiErrorDialogAt = now;
    _showAiErrorDetails(title: title, fallback: fallback);
  }

  Future<void> _cleanupAudioSessions({bool stopTts = true}) async {
    try {
      if (stopTts) await _tts.stop();
    } catch (_) {
      // ignore
    }

    try {
      if (_speechToText.isListening) {
        await _speechToText.stop();
      }
    } catch (_) {
      // ignore
    }

    try {
      if (!kIsWeb && await _audioRecorder.isRecording()) {
        await _audioRecorder.stop();
      }
    } catch (_) {
      // ignore
    }

    try {
      await _audioPlayer.stop();
    } catch (_) {
      // ignore
    }
  }

  Future<void> _playLocalAudioFile(String path, {required String label}) async {
    if (kIsWeb) {
      if (!mounted) return;
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text('$label: 웹에서는 다시 듣기를 지원하지 않아요.')));
      return;
    }

    final p = path.trim();
    if (p.isEmpty) {
      if (!mounted) return;
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text('$label: 재생할 파일 경로가 없어요.')));
      return;
    }

    bool exists = false;
    for (int i = 0; i < 3; i++) {
      exists = await File(p).exists();
      if (exists) break;
      await Future.delayed(Duration(milliseconds: 180 * (i + 1)));
    }

    if (!exists) {
      debugPrint('[play_audio][$label] missing file path=$p');
      if (!mounted) return;
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text('$label: 녹음 파일이 없어요.')));
      return;
    }

    try {
      await _audioPlayer.stop();
    } catch (_) {
      // ignore
    }

    try {
      await _audioPlayer.play(DeviceFileSource(p));
    } catch (e) {
      debugPrint('[play_audio][$label] err=$e');
      if (!mounted) return;
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text('$label: 재생 실패: $e')));
    }
  }

  String _sanitizeTipKorean(String tip) {
    final base = tip.trim();
    if (base.isEmpty) return base;

    final allowed = <String>{
      ...widget.scenario.keywords.map((e) => e.toLowerCase().trim()),
      ..._tokenSetFromEnglish(widget.scenario.questionEn),
    }..removeWhere((e) => e.isEmpty);
    if (allowed.isEmpty) return base;

    return base
        .replaceAllMapped(RegExp(r"\b[A-Za-z][A-Za-z']*\b"), (m) {
          final w = m.group(0) ?? '';
          final key = w.toLowerCase();
          if (allowed.contains(key)) return w;
          return '';
        })
        .replaceAll(RegExp(r'\s{2,}'), ' ')
        .trim();
  }

  GameState _gameState = GameState.ready;
  int _counter = 3;
  Timer? _timer;
  final GameProvider _gameProvider = GameProvider();
  int _lastGainedPoints = 0;
  int _round = 1;
  int _turnInRound = 0;
  late final int _maxRounds;

  String _currentQuestion = "화면을 탭하여 시작!";
  String _currentQuestionKorean = "";
  String _currentCategory = "준비";
  String _currentGuide = "";
  String _currentMeaning = "";
  String _currentTip = "";
  String _currentFailReason = "";

  String _processingMessage = "AI 분석 중... ⏳";

  String _userSpokenText = "";
  bool _isSuccess = false;

  bool _isPracticing = false;
  bool _isPracticeResult = false;
  int _practiceScorePercent = 0;
  int _practiceTextScorePercent = 0;
  int _practiceFluencyScorePercent = 0;
  String _practiceRecognizedText = "";
  String _practiceDebugInfo = '';
  bool _showPracticeDebugInfo = false;
  DateTime? _practiceStartAt;
  Duration _practiceAudioDuration = Duration.zero;
  String? _practiceAudioPath;
  bool _sttReady = false;
  bool _voiceApplied = false;
  Future<void>? _voiceApplyFuture;
  String? _activeAnswerRecordPath;
  String? _activePracticeRecordPath;
  String? _lastAnswerAudioPath;
  String _liveAnswerSttText = "";
  DateTime? _answerRecordStartAt;

  void _appendPracticeDebug(String msg) {
    final m = msg.trim();
    if (m.isEmpty) return;
    if (!mounted) return;
    setState(() {
      final base = _practiceDebugInfo.trim();
      if (base.isEmpty) {
        _practiceDebugInfo = m;
      } else {
        final next = '$base\n$m';
        _practiceDebugInfo = next.length > 1400
            ? next.substring(next.length - 1400)
            : next;
      }
    });
  }

  String _turnDiag = '';

  bool _canContinueAfterFail = false;
  String _continueNextNpcLine = '';

  final List<Map<String, String>> _conversation = [];

  static const Map<String, String> _keywordGlossKo = {
    'coffee': '커피',
    'order': '주문',
    'wrong': '틀린/잘못된',
    'hot': '뜨거운',
    'cold': '차가운',
    'milk': '우유',
    'sugar': '설탕',
    'passport': '여권',
    'baggage': '수하물',
    'lost': '분실',
    'missing': '없어진/누락된',
    'gate': '게이트',
    'flight': '비행편',
    'reservation': '예약',
    'room': '객실/방',
    'key': '열쇠/키',
    'status': '상태/진행 상황',
    'update': '업데이트/보고',
    'deadline': '마감',
    'risk': '위험',
    'noise': '소음',
    'package': '택배',
    'delivery': '배송',
  };

  String? _lastLocalFallbackOpeningLine;

  static const int _nextScenarioQueueSize = 3;
  final List<Scenario> _nextScenarioQueue = [];

  DateTime? _lastPreloadAt;
  bool _preloadInFlight = false;

  static final Map<String, List<String>> _recentScenarioIdsByKey = {};

  String _scenarioHistoryKey() => '${widget.theme.id}:${levelId(widget.level)}';

  void _rememberScenarioId(String id) {
    final key = _scenarioHistoryKey();
    final list = _recentScenarioIdsByKey[key] ??= <String>[];
    list.remove(id);
    list.add(id);
    while (list.length > 12) {
      list.removeAt(0);
    }
  }

  Set<String> _recentScenarioIdsSet({int keep = 12}) {
    final key = _scenarioHistoryKey();
    final list = _recentScenarioIdsByKey[key] ?? const <String>[];
    if (list.isEmpty) return <String>{};
    final start = list.length - keep;
    return list.sublist(start < 0 ? 0 : start).toSet();
  }

  static const List<String> _processingMessages = [
    "AI 심사관이 째려보는 중... 👀",
    "AI가 문맥을 캐묻는 중... 🔎",
    "AI가 태클 걸 포인트 찾는 중... 🧾",
    "AI가 점수표를 들여다보는 중... 📋",
    "AI가 단어 선택을 검문 중... 🚨",
  ];

  String _pickProcessingMessage() {
    if (_processingMessages.isEmpty) return "AI 분석 중... ⏳";
    return _processingMessages[Random().nextInt(_processingMessages.length)];
  }

  @override
  void initState() {
    super.initState();
    _maxRounds = maxRoundsForLevel(widget.level);
    _rememberScenarioId(widget.scenario.id);
    _initSystems();
    _backgroundPreload();
    _initBannerAd();
  }

  void _initBannerAd() {
    final unitId = _bannerAdUnitId();
    if (unitId.trim().isEmpty) return;
    final ad = BannerAd(
      adUnitId: unitId,
      size: AdSize.banner,
      request: const AdRequest(),
      listener: BannerAdListener(
        onAdLoaded: (_) {
          if (!mounted) return;
          setState(() => _bannerReady = true);
          logEventSafe(
            'banner_loaded',
            parameters: {
              'screen': 'game',
              'theme_id': widget.theme.id,
              'level': levelId(widget.level),
            },
          );
        },
        onAdFailedToLoad: (ad, err) {
          debugPrint('[Ad_Banner][Game] failed $err');
          logEventSafe(
            'banner_failed',
            parameters: {
              'screen': 'game',
              'theme_id': widget.theme.id,
              'level': levelId(widget.level),
              'err': err.toString(),
            },
          );
          ad.dispose();
        },
      ),
    );
    _banner = ad;
    unawaited(ad.load());
  }

  String _bannerAdUnitId() {
    if (kIsWeb) return '';
    if (Platform.isAndroid) return 'ca-app-pub-3940256099942544/6300978111';
    if (Platform.isIOS) return 'ca-app-pub-3940256099942544/2934735716';
    return '';
  }

  void _initSystems() async {
    await _tts.setLanguage("en-US");
    await _tts.setSpeechRate(0.52);
    await _tts.setVolume(1.0);

    unawaited(_ensureBestVoiceApplied());

    _tts.setCompletionHandler(() {
      if (_gameState == GameState.speakingQuestion) {
        _start3SecondTimer();
      }
    });
    await _audioRecorder.hasPermission();

    try {
      _sttReady = await _speechToText.initialize();
    } catch (_) {
      _sttReady = false;
    }
  }

  @override
  void dispose() {
    unawaited(_cleanupAudioSessions());
    unawaited(_audioPlayer.dispose());
    unawaited(_speechToText.stop());
    _timer?.cancel();
    _banner?.dispose();
    _interstitial?.dispose();
    super.dispose();
  }

  Future<int> _getDeathCount() async {
    final prefs = await SharedPreferences.getInstance();
    return prefs.getInt(_prefsDeathCountKey) ?? 0;
  }

  Future<void> _setDeathCount(int v) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setInt(_prefsDeathCountKey, v);
  }

  Future<void> _incrementDeathCount() async {
    final current = await _getDeathCount();
    await _setDeathCount(current + 1);
  }

  String _interstitialUnitId() {
    if (kIsWeb) return '';
    if (Platform.isAndroid) return 'ca-app-pub-3940256099942544/1033173712';
    if (Platform.isIOS) return 'ca-app-pub-3940256099942544/4411468910';
    return '';
  }

  Future<InterstitialAd?> _loadInterstitial() async {
    final unitId = _interstitialUnitId();
    if (unitId.trim().isEmpty) return null;
    if (_interstitial != null) return _interstitial;
    if (_interstitialLoading) return null;
    _interstitialLoading = true;
    final completer = Completer<InterstitialAd?>();
    InterstitialAd.load(
      adUnitId: unitId,
      request: const AdRequest(),
      adLoadCallback: InterstitialAdLoadCallback(
        onAdLoaded: (ad) {
          _interstitialLoading = false;
          _interstitial = ad;
          logEventSafe(
            'interstitial_loaded',
            parameters: {
              'theme_id': widget.theme.id,
              'level': levelId(widget.level),
            },
          );
          ad.fullScreenContentCallback = FullScreenContentCallback(
            onAdDismissedFullScreenContent: (ad) {
              debugPrint('[Ad_Interstitial] dismissed');
              logEventSafe(
                'interstitial_dismissed',
                parameters: {
                  'theme_id': widget.theme.id,
                  'level': levelId(widget.level),
                },
              );
              ad.dispose();
              if (identical(_interstitial, ad)) _interstitial = null;
            },
            onAdFailedToShowFullScreenContent: (ad, err) {
              debugPrint('[Ad_Interstitial] show_failed $err');
              logEventSafe(
                'interstitial_show_failed',
                parameters: {
                  'theme_id': widget.theme.id,
                  'level': levelId(widget.level),
                  'err': err.toString(),
                },
              );
              ad.dispose();
              if (identical(_interstitial, ad)) _interstitial = null;
            },
          );
          completer.complete(ad);
        },
        onAdFailedToLoad: (err) {
          debugPrint('[Ad_Interstitial] load_failed $err');
          logEventSafe(
            'interstitial_failed_load',
            parameters: {
              'theme_id': widget.theme.id,
              'level': levelId(widget.level),
              'err': err.toString(),
            },
          );
          _interstitialLoading = false;
          completer.complete(null);
        },
      ),
    );
    return completer.future;
  }

  Future<void> _maybeShowInterstitialOnLobbyReturn() async {
    final deaths = await _getDeathCount();
    if (deaths < 3) return;
    await _setDeathCount(deaths - 3);
    final ad = await _loadInterstitial();
    if (ad == null) return;
    try {
      logEventSafe(
        'interstitial_show',
        parameters: {
          'theme_id': widget.theme.id,
          'level': levelId(widget.level),
          'deaths_before': deaths,
        },
      );
      await ad.show();
    } catch (e) {
      debugPrint('[Ad_Interstitial] show_exception $e');
    }
  }

  Future<void> _returnToLobby() async {
    logEventSafe(
      'return_to_lobby',
      parameters: {'theme_id': widget.theme.id, 'level': levelId(widget.level)},
    );

    unawaited(() async {
      try {
        await _uploadWeeklyBestScore(
          level: widget.level,
          score: _gameProvider.score,
          themeId: widget.theme.id,
        );
      } catch (e) {
        debugPrint('[weekly_leaderboard] upload_failed err=$e');
      }
    }());

    await _maybeShowInterstitialOnLobbyReturn();
    if (!mounted) return;
    Navigator.of(context).popUntil((r) => r.isFirst);
  }

  Future<void> _ensureBestVoiceApplied() {
    if (_voiceApplied) return Future.value();
    return _voiceApplyFuture ??= _applyBestVoiceWithRetry();
  }

  Future<void> _applyBestVoiceWithRetry() async {
    for (int i = 0; i < 20; i++) {
      List<dynamic>? voices = await _tts.getVoices;
      if (voices != null && voices.isNotEmpty) {
        try {
          var bestVoice = voices.firstWhere(
            (v) => v['name'].toString().contains('Google US English'),
            orElse: () => voices.firstWhere(
              (v) => v['name'].toString().toLowerCase().contains(
                RegExp(r'google|premium|enhanced|natural|siri'),
              ),
              orElse: () => voices.firstWhere(
                (v) => v['locale'].toString().contains('en-US'),
                orElse: () => voices.first,
              ),
            ),
          );
          await _tts.setVoice({
            "name": bestVoice['name'],
            "locale": bestVoice['locale'],
          });
          _voiceApplied = true;
          if (bestVoice['name'].toString().contains('Google')) return;
        } catch (_) {
          // ignore
        }
      }
      await Future.delayed(const Duration(milliseconds: 120));
    }
  }

  Future<void> _speakText(String text) async {
    await _ensureBestVoiceApplied();
    final rawRate = switch (widget.level) {
      SurvivalLevel.beginner => 0.46,
      SurvivalLevel.intermediate => 0.50,
      SurvivalLevel.advanced => 0.62,
      SurvivalLevel.hardcore => 0.74,
    };
    final rate = rawRate.clamp(0.35, 0.95);
    await _tts.setSpeechRate(rate.toDouble());
    await _tts.speak(text);
  }

  String _fallbackGuideForLine(String npcLine) {
    final s = npcLine.trim().toLowerCase();
    if (s.contains('cream or sugar') ||
        (s.contains('cream') && s.contains('sugar'))) {
      return 'Cream, please.';
    }
    if (s.contains('how many sugar')) {
      return 'Two sugars, please.';
    }
    if (s.contains('what kind of coffee')) {
      return "I'd like an iced latte, please.";
    }
    if (s.contains('whipped cream') || s.contains('whip cream')) {
      return 'No, thanks.';
    }
    if (s.contains('buzzer') || s.contains('vibrate')) {
      return "Okay, got it. Thanks.";
    }
    if (s.contains('reusable cup') || s.contains('paper cup')) {
      return 'A paper cup, please.';
    }
    if (s.contains('oat milk')) {
      return 'Soy is fine, thanks.';
    }
    if (s.contains('iced') || s.contains('hot')) {
      return 'I’ll take it iced, please.';
    }
    if (s.contains('to-go') || s.contains('here')) {
      return 'To-go, please.';
    }
    if (s.contains("what's wrong") || s.contains('whats wrong')) {
      return "I think my order is wrong. Could you check it, please?";
    }
    if (s.contains('refund')) {
      return "This isn't what I ordered. Can I get a refund, please?";
    }
    if (s.contains('remake') || s.contains('make it again')) {
      return "This isn't what I ordered. Could you remake it, please?";
    }
    return _fallbackExampleAnswer();
  }

  String _fallbackMeaningForLine(String npcLine) {
    final s = npcLine.trim().toLowerCase();
    if (s.contains('cream or sugar') ||
        (s.contains('cream') && s.contains('sugar'))) {
      return '크림으로 주세요.';
    }
    if (s.contains('how many sugar')) {
      return '설탕 2개로 주세요.';
    }
    if (s.contains('what kind of coffee')) {
      return '아이스 라떼로 주세요.';
    }
    if (s.contains('whipped cream') || s.contains('whip cream')) {
      return '아니요, 괜찮아요.';
    }
    if (s.contains('buzzer') || s.contains('vibrate')) {
      return '네, 알겠어요. 감사합니다.';
    }
    if (s.contains('reusable cup') || s.contains('paper cup')) {
      return '종이컵으로 주세요.';
    }
    if (s.contains('oat milk')) {
      return '두유로 괜찮아요, 감사합니다.';
    }
    if (s.contains('iced') || s.contains('hot')) {
      return '아이스로 주세요.';
    }
    if (s.contains('to-go') || s.contains('here')) {
      return '포장으로 주세요.';
    }
    if (s.contains("what's wrong") || s.contains('whats wrong')) {
      return '주문이 잘못 나온 것 같아요. 확인해 주실 수 있나요?';
    }
    if (s.contains('refund')) {
      return '이건 제가 주문한 게 아니에요. 환불 가능할까요?';
    }
    if (s.contains('remake') || s.contains('make it again')) {
      return '이건 제가 주문한 게 아니에요. 다시 만들어 주실 수 있나요?';
    }
    return _fallbackExampleAnswerKorean();
  }

  String _styleNpcByLevel(String base) {
    final s = base.trim();
    if (s.isEmpty) return s;
    return switch (widget.level) {
      SurvivalLevel.beginner => s,
      SurvivalLevel.intermediate =>
        s.replaceAll('Do you', "D'you").replaceAll('want to', 'wanna'),
      SurvivalLevel.advanced => s,
      SurvivalLevel.hardcore => s,
    };
  }

  String _styleCoachingByLevel(String base) {
    final s = base.trim();
    if (s.isEmpty) return s;
    return switch (widget.level) {
      SurvivalLevel.beginner => s,
      SurvivalLevel.intermediate =>
        s.replaceAll('Could you', 'Can you').replaceAll('please?', 'please?'),
      SurvivalLevel.advanced => s,
      SurvivalLevel.hardcore => s,
    };
  }

  String _localFallbackNpcNext(String spokenText) {
    final keywords = _requiredKeywordsForCurrentScenario();
    final s = spokenText.trim().toLowerCase();
    final missing = keywords
        .where((k) => k.trim().isNotEmpty)
        .where((k) => !s.contains(k.toLowerCase()))
        .toList();

    if (widget.theme.id == 'cafe') {
      if (missing.contains('refund')) {
        return _styleNpcByLevel('Do you want a refund or a remake?');
      }
      if (missing.contains('wrong') || missing.contains('order')) {
        return _styleNpcByLevel("What's wrong with it exactly?");
      }
      if (missing.contains('hot') || missing.contains('cold')) {
        return _styleNpcByLevel('Did you want it hot or iced?');
      }
    }

    if (widget.theme.id == 'airport') {
      if (missing.contains('passport')) {
        return _styleNpcByLevel('Can I see your passport, please?');
      }
      if (missing.contains('baggage') || missing.contains('lost')) {
        return _styleNpcByLevel("When did you last see your bag?");
      }
      if (missing.contains('visit') || missing.contains('purpose')) {
        return _styleNpcByLevel("What's the purpose of your trip?");
      }
      return _styleNpcByLevel(
        'Which terminal or counter are you at right now?',
      );
    }

    if (widget.theme.id == 'hotel') {
      if (missing.contains('reservation') || missing.contains('room')) {
        return _styleNpcByLevel('What name is the reservation under?');
      }
      if (missing.contains('key')) {
        return _styleNpcByLevel(
          'Did you lose your key card, or is it not working?',
        );
      }
      return _styleNpcByLevel('What seems to be the issue with your room?');
    }

    if (widget.theme.id == 'hospital') {
      if (missing.contains('pain')) {
        return _styleNpcByLevel(
          'Where does it hurt, and how long has it been?',
        );
      }
      return _styleNpcByLevel('What symptoms are you having right now?');
    }

    if (widget.theme.id == 'workplace') {
      if (missing.contains('status') || missing.contains('update')) {
        return _styleNpcByLevel("What's the status—are we on track?");
      }
      if (missing.contains('deadline')) {
        return _styleNpcByLevel('What deadline are we aiming for?');
      }
      if (missing.contains('risk')) {
        return _styleNpcByLevel('What’s the biggest risk right now?');
      }
      return _styleNpcByLevel('What do you need from me to move forward?');
    }

    if (widget.theme.id == 'neighborhood') {
      if (missing.contains('noise')) {
        return _styleNpcByLevel('What time does the noise usually happen?');
      }
      if (missing.contains('package') || missing.contains('delivery')) {
        return _styleNpcByLevel(
          'Do you have a tracking number or a photo proof?',
        );
      }
      return _styleNpcByLevel(
        'Can you tell me what happened, in one sentence?',
      );
    }

    if (widget.theme.id == 'directions') {
      return _styleNpcByLevel(
        'Where are you trying to go, and where are you now?',
      );
    }

    if (widget.theme.id == 'interview') {
      return _styleNpcByLevel(
        'Can you give me a concrete example from your experience?',
      );
    }

    if (widget.theme.id == 'party') {
      return _styleNpcByLevel("Oh nice—so how do you know the host?");
    }

    if (widget.theme.id == 'clothing') {
      return _styleNpcByLevel('What size are you looking for, and what color?');
    }

    final generic = switch (widget.level) {
      SurvivalLevel.beginner => const <String>[
        'Can you be more specific?',
        'What exactly do you mean?',
        'Tell me one more detail.',
        'What would you like me to do?',
        'So what happened?',
      ],
      SurvivalLevel.intermediate => const <String>[
        "Okay—what's the deal here?",
        "Gotcha. What happened exactly?",
        "Alright, gimme one more detail.",
        "What do you wanna do—refund or remake?",
        "Just to confirm, what did you ask for?",
      ],
      SurvivalLevel.advanced => const <String>[
        "Walk me through what went wrong, real quick.",
        "What's the main issue—taste, temperature, or the order itself?",
        "Help me understand: what were you expecting versus what you got?",
        "What would make this right for you?",
        "Okay, what's the key detail I'm missing?",
      ],
      SurvivalLevel.hardcore => const <String>[
        "Alright—what's the actual problem?",
        "What'd you order, and what'd you get?",
        "Make it quick—what do you want me to do?",
        "So what's the move—refund or remake?",
        "Spell it out. What's wrong with it?",
      ],
    };
    return generic[Random().nextInt(generic.length)];
  }

  String _rescueQuestionAfterNonAnswer() {
    return switch (widget.theme.id) {
      'cafe' => _styleNpcByLevel('No problem. What can I get you today?'),
      'airport' => _styleNpcByLevel(
        'No worries. What seems to be the problem?',
      ),
      'hotel' => _styleNpcByLevel('No problem. How can I help you today?'),
      'hospital' => _styleNpcByLevel('It’s okay. What’s bothering you?'),
      'clothing' => _styleNpcByLevel('No problem. What are you looking for?'),
      'interview' => _styleNpcByLevel('No problem. Can you tell me more?'),
      'directions' => _styleNpcByLevel(
        'No problem. Where are you trying to go?',
      ),
      'party' => _styleNpcByLevel('No problem. So what’s going on?'),
      'neighborhood' => _styleNpcByLevel(
        'No problem. What’s the issue you’re having?',
      ),
      'workplace' => _styleNpcByLevel('No problem. What do you need from me?'),
      _ => _styleNpcByLevel('No problem. What can I help you with?'),
    };
  }

  GeminiTurnJudgement _sanitizeJudgement(GeminiTurnJudgement j) {
    final q = _currentQuestion.trim().toLowerCase();
    final ideal = j.idealAnswer.trim();
    final idealLower = ideal.toLowerCase();
    final copied = ideal.isNotEmpty && q.isNotEmpty && idealLower == q;
    if (!copied) return j;
    return GeminiTurnJudgement(
      contextScore: j.contextScore,
      relevant: j.relevant,
      reasonKorean: j.reasonKorean,
      npcNextLine: j.npcNextLine,
      idealAnswer: _fallbackGuideForLine(_currentQuestion),
      idealAnswerKorean: _fallbackMeaningForLine(_currentQuestion),
      tipKorean: j.tipKorean.trim().isEmpty
          ? _fallbackTipKorean()
          : j.tipKorean,
    );
  }

  Future<void> _startNpcConversation() async {
    _timer?.cancel();
    await _cleanupAudioSessions();

    setState(() {
      _gameState = GameState.processing;
      _processingMessage = _pickProcessingMessage();
      _currentCategory =
          "${widget.theme.emoji} ${widget.theme.label} · ${levelLabel(widget.level)}";
      _currentQuestion = "NPC 준비 중...";
      _currentQuestionKorean = "";
      _currentGuide = "";
      _currentMeaning = "";
      _currentTip = "";
      _currentFailReason = "";
      _canContinueAfterFail = false;
      _continueNextNpcLine = '';
      _userSpokenText = "";
      _isPracticing = false;
      _isPracticeResult = false;
      _practiceScorePercent = 0;
      _practiceTextScorePercent = 0;
      _practiceFluencyScorePercent = 0;
      _practiceRecognizedText = "";
      _practiceStartAt = null;
      _practiceAudioDuration = Duration.zero;
      _practiceAudioPath = null;

      _turnInRound = 0;
      _round = 1;
      _gameProvider.resetRun();
      _lastGainedPoints = 0;
      _turnDiag = '';
      _conversation.clear();
    });

    final opening = await _ai.generateOpeningLine(
      theme: widget.theme,
      level: widget.level,
      scenario: widget.scenario,
    );
    if (opening == null) {
      _showAiErrorDetails(
        title: 'AI 통신 실패 (오프닝)',
        fallback:
            'opening=null status=${_ai.lastHttpStatus} ex=${_ai.lastException} body=${(_ai.lastHttpBody ?? '').trim()}'
                .trim(),
      );
    }
    final scenarioFallback = widget.scenario.questionEn.trim();
    final useScenarioFallback =
        scenarioFallback.isNotEmpty &&
        scenarioFallback != _lastLocalFallbackOpeningLine;
    String firstLine =
        opening ??
        (useScenarioFallback ? scenarioFallback : _fallbackOpeningLine());
    if (_isBadNpcLine(firstLine)) {
      final better = useScenarioFallback
          ? scenarioFallback
          : _fallbackOpeningLine();
      firstLine = better;
    }
    _conversation.add({"role": "npc", "text": firstLine});

    setState(() {
      _gameState = GameState.speakingQuestion;
      _currentQuestion = firstLine;
    });

    await _setQuestionAndSpeak(firstLine, newTurn: true);
  }

  Future<void> _setQuestionAndSpeak(String line, {bool newTurn = false}) async {
    if (newTurn) {
      _gameProvider.startNewTurn();
    }
    setState(() {
      _gameState = GameState.speakingQuestion;
      _currentQuestion = line;
      _currentQuestionKorean = "";
    });

    String? ko;
    for (int attempt = 0; attempt < 3; attempt++) {
      final extra = switch (attempt) {
        0 => '',
        1 =>
          '\n\n(IMPORTANT: translate the FULL sentence naturally in Korean. Do not truncate. End the sentence properly.)',
        _ =>
          '\n\n(IMPORTANT: Natural Korean. No truncation. No English. Finish with proper ending/punctuation.)',
      };
      ko = await _ai.translateToKorean('$line$extra');
      ko = (ko ?? '').trim();
      if (_isGoodKoreanTranslation(ko, line)) break;
      if (_ai.lastHttpStatus == 429) break;
    }
    if ((ko ?? '').trim().isEmpty) {
      _showAiErrorDetails(
        title: 'AI 통신 실패 (번역)',
        fallback:
            'translate="" status=${_ai.lastHttpStatus} ex=${_ai.lastException} body=${(_ai.lastHttpBody ?? '').trim()}'
                .trim(),
      );
    }
    if (mounted) {
      setState(() {
        _currentQuestionKorean = _isGoodKoreanTranslation(ko ?? '', line)
            ? ko!.trim()
            : _fallbackQuestionKorean(line);
      });
    }

    _speakText(_currentQuestion);
  }

  Set<String> _tokenSetFromEnglish(String s) {
    final lower = s.toLowerCase();
    final cleaned = lower.replaceAll(RegExp(r"[^a-z0-9'\s]"), ' ');
    return cleaned
        .split(RegExp(r'\s+'))
        .map((e) => e.trim())
        .where((e) => e.length >= 2)
        .toSet();
  }

  Set<String> _allowedKeywordSetForTips({required String idealAnswer}) {
    final fromQuestion = _tokenSetFromEnglish(_currentQuestion);
    final fromIdeal = _tokenSetFromEnglish(idealAnswer);
    return <String>{...fromQuestion, ...fromIdeal};
  }

  void _backgroundPreload() {
    // Intentionally no-op in this build. This hook remains to keep
    // the game loop stable even when AI/network is failing.

    if (_preloadInFlight) return;
    final now = DateTime.now();
    final last = _lastPreloadAt;
    if (last != null && now.difference(last).inSeconds < 6) return;
    _lastPreloadAt = now;

    _preloadInFlight = true;
    unawaited(() async {
      try {
        final themeId = widget.theme.id;
        final level = widget.level;

        debugPrint(
          '[Scenario_Prefetch] start theme=$themeId level=${levelId(level)}',
        );

        logEventSafe(
          'scenario_prefetch_start',
          parameters: {'theme_id': themeId, 'level': levelId(level)},
        );

        await contentStore.prefetchScenarios(
          themeId: themeId,
          level: level,
          minCount: 10,
        );

        final pool = contentStore.getScenarios(themeId, level);
        if (pool.isEmpty) return;

        final recent = _recentScenarioIdsSet(keep: 12)..add(widget.scenario.id);
        final candidates = pool.where((s) => !recent.contains(s.id)).toList();
        final src = candidates.isNotEmpty ? candidates : pool;

        while (_nextScenarioQueue.length < _nextScenarioQueueSize) {
          final picked = _pickRandomScenario(
            src,
            excludeId: widget.scenario.id,
            avoidIds: recent,
          );
          if (_nextScenarioQueue.any((e) => e.id == picked.id)) {
            if (src.length <= 1) break;
            continue;
          }
          _nextScenarioQueue.add(picked);
          _rememberScenarioId(picked.id);
          if (_nextScenarioQueue.length >= src.length) break;
        }

        debugPrint(
          '[Scenario_Prefetch] done pool=${pool.length} nextQ=${_nextScenarioQueue.length}',
        );

        logEventSafe(
          'scenario_prefetch_done',
          parameters: {
            'theme_id': themeId,
            'level': levelId(level),
            'pool': pool.length,
            'next_q': _nextScenarioQueue.length,
          },
        );
      } catch (e) {
        debugPrint('[Scenario_Prefetch] err=$e');
        logEventSafe(
          'scenario_prefetch_err',
          parameters: {
            'theme_id': widget.theme.id,
            'level': levelId(widget.level),
            'err': e.toString(),
          },
        );
      } finally {
        _preloadInFlight = false;
      }
    }());
  }

  String _decorateTipWithGloss(String baseTip, {required String idealAnswer}) {
    final tip = baseTip
        .split('\n')
        .where((line) => !line.contains('핵심 단어:'))
        .join('\n')
        .trim();

    final allowed = _allowedKeywordSetForTips(idealAnswer: idealAnswer);
    final kws = _requiredKeywordsForCurrentScenario()
        .where((k) => allowed.contains(k.trim().toLowerCase()))
        .toList();
    if (kws.isEmpty) return tip;

    final pairs = <String>[];
    for (final k in kws) {
      final key = k.trim().toLowerCase();
      if (key.isEmpty) continue;
      final ko = _keywordGlossKo[key];
      if (ko == null || ko.trim().isEmpty) continue;
      pairs.add('$key=$ko');
      if (pairs.length >= 6) break;
    }
    if (pairs.isEmpty) return tip;
    final gloss = '핵심 단어: ${pairs.join(', ')}';
    if (tip.isEmpty) return gloss;
    return '$tip\n\n$gloss';
  }

  String _fallbackQuestionKorean(String englishLine) {
    final s = englishLine.trim().toLowerCase();
    if (s.isEmpty) return "";

    if (s.contains("what's wrong with your order") ||
        s.contains("whats wrong with your order")) {
      return "주문에 어떤 문제가 있나요?";
    }
    if (s.contains("what's the problem") || s.contains("whats the problem")) {
      return "무슨 문제가 있나요?";
    }
    if (s.contains("status update")) {
      return "지금 상황(진행 상황) 보고해 주세요.";
    }
    if (s.contains("baggage") || s.contains("bag")) {
      return "수하물 문제 상황을 말해 주세요.";
    }

    return switch (widget.theme.id) {
      'cafe' => "주문 상황을 설명해 주세요.",
      'airport' => "공항에서 무슨 일이 있었는지 말해 주세요.",
      'workplace' => "현재 업무 상황을 간단히 말해 주세요.",
      'neighborhood' => "어떤 문제가 있는지 말해 주세요.",
      _ => "어떤 문제인지 말해 주세요.",
    };
  }

  String _fallbackOpeningLine() {
    final byTheme = switch (widget.theme.id) {
      'cafe' => switch (widget.level) {
        SurvivalLevel.beginner => const <String>[
          "What's wrong with your order?",
          "Do you want it hot or iced?",
          "To-go or for here?",
          "Would you like whipped cream?",
        ],
        SurvivalLevel.intermediate => const <String>[
          "Hey—what's up with your order?",
          "So, hot or iced today?",
          "You want it to-go or here?",
          "Wanna add whipped cream on top?",
        ],
        SurvivalLevel.advanced => const <String>[
          "Alright, what seems to be the issue with your drink?",
          "Before I make it—hot, iced, or something in-between?",
          "Is this to-go, or are you staying here?",
          "Any extras—whipped cream, syrup, anything like that?",
        ],
        SurvivalLevel.hardcore => const <String>[
          "Yo—what's wrong with the order?",
          "Hot or iced—quick.",
          "To-go or here?",
          "You want whip on that or nah?",
        ],
      },
      'airport' => switch (widget.level) {
        SurvivalLevel.beginner => const <String>[
          "What happened with your baggage?",
          "Do you have your passport with you?",
          "Where are you flying to today?",
          "What's the purpose of your trip?",
        ],
        SurvivalLevel.intermediate => const <String>[
          "So—what happened to your bag?",
          "Can I see your passport real quick?",
          "Where're you headed today?",
          "What's the purpose of your trip?",
        ],
        SurvivalLevel.advanced => const <String>[
          "Walk me through what happened to your baggage.",
          "May I see your passport and boarding pass?",
          "Where are you headed, and for how long?",
          "What brings you here today—business or leisure?",
        ],
        SurvivalLevel.hardcore => const <String>[
          "Alright—what's the deal with your bag?",
          "Passport. Now.",
          "Where you headed?",
          "Purpose of the trip?",
        ],
      },
      'workplace' => switch (widget.level) {
        SurvivalLevel.beginner => const <String>[
          "Give me a quick status update.",
          "Are we on schedule?",
          "What do you need help with?",
          "What's the biggest problem right now?",
        ],
        SurvivalLevel.intermediate => const <String>[
          "Alright—quick status update?",
          "Are we still on track?",
          "What's blocking you right now?",
          "What do you need from me?",
        ],
        SurvivalLevel.advanced => const <String>[
          "Give me a concise status update, including risks.",
          "Are we on track for the deadline, realistically?",
          "What's your biggest blocker, and what's your plan?",
          "What support do you need to unblock this?",
        ],
        SurvivalLevel.hardcore => const <String>[
          "Status. Now.",
          "Are we shipping or not?",
          "What's the blocker?",
          "What do you need—quick?",
        ],
      },
      _ => switch (widget.level) {
        SurvivalLevel.beginner => const <String>[
          "What's the problem?",
          "What happened?",
          "Can you help me, please?",
          "What should we do now?",
        ],
        SurvivalLevel.intermediate => const <String>[
          "Okay—what's going on?",
          "So what's the problem here?",
          "Can you help me out?",
          "What do we do next?",
        ],
        SurvivalLevel.advanced => const <String>[
          "What's going on, exactly?",
          "Help me understand the situation.",
          "What outcome are you looking for?",
          "What's the best next step here?",
        ],
        SurvivalLevel.hardcore => const <String>[
          "What's going on—quick.",
          "What's the problem?",
          "What do you want me to do?",
          "So what's the move?",
        ],
      },
    };
    final rnd = Random();
    if (byTheme.isEmpty) return "What's the problem?";
    if (byTheme.length == 1) return byTheme.first;

    String pick = byTheme[rnd.nextInt(byTheme.length)];
    int safety = 0;
    while (safety < 8 &&
        _lastLocalFallbackOpeningLine != null &&
        pick == _lastLocalFallbackOpeningLine) {
      safety++;
      pick = byTheme[rnd.nextInt(byTheme.length)];
    }
    _lastLocalFallbackOpeningLine = pick;
    return pick;
  }

  String _fallbackExampleAnswer() {
    switch (widget.theme.id) {
      case 'cafe':
        return "I ordered a coffee, but this isn't what I asked for.";
      case 'airport':
        return "My baggage didn't arrive. What should I do next?";
      case 'workplace':
        return "I'm making progress, but there's a risk we might miss the deadline.";
      case 'neighborhood':
      default:
        return "I'm sorry, but there's a problem I need help with.";
    }
  }

  String _fallbackExampleAnswerKorean() {
    switch (widget.theme.id) {
      case 'cafe':
        return "커피를 주문했는데 제가 요청한 것과 달라요.";
      case 'airport':
        return "제 수하물이 도착하지 않았어요. 다음에 어떻게 해야 하나요?";
      case 'workplace':
        return "진행은 되고 있지만 마감에 늦을 위험이 있어요.";
      case 'neighborhood':
      default:
        return "죄송하지만 도움이 필요한 문제가 있어요.";
    }
  }

  String _fallbackTipKorean() {
    switch (widget.theme.id) {
      case 'cafe':
        return "주문한 메뉴 + 문제(차가움/다름) + 요청(다시 만들어주세요)을 한 문장에 넣어보세요.";
      case 'airport':
        return "무엇이 없어졌는지 + 언제/어디서 + 다음 조치를 질문하면 통과 확률이 올라가요.";
      case 'workplace':
        return "현재 상태 + 리스크 + 필요한 지원(도움)을 말하면 더 자연스러워요.";
      case 'neighborhood':
      default:
        return "문제 상황 + 요청(해결/확인)을 짧게라도 명확히 말하면 좋아요.";
    }
  }

  void _start3SecondTimer() {
    setState(() {
      _gameState = GameState.waitingForTap;
      _counter = 3;
    });

    _timer = Timer.periodic(const Duration(seconds: 1), (timer) {
      if (!mounted) return;
      setState(() {
        if (_counter > 1) {
          _counter--;
        } else {
          timer.cancel();
          unawaited(_handleTimeoutFailure());
        }
      });
    });
  }

  Future<void> _handleTimeoutFailure() async {
    if (!mounted) return;

    logEventSafe(
      'turn_timeout',
      parameters: {
        'theme_id': widget.theme.id,
        'level': levelId(widget.level),
        'scenario_id': widget.scenario.id,
        'score': _gameProvider.score,
        'turn_in_round': _turnInRound,
        'round': _round,
      },
    );

    setState(() {
      _gameState = GameState.processing;
      _processingMessage = _pickProcessingMessage();
    });

    final passThreshold = switch (widget.level) {
      SurvivalLevel.beginner => 70,
      SurvivalLevel.intermediate => 80,
      SurvivalLevel.advanced => 90,
      SurvivalLevel.hardcore => 100,
    };

    await _applyFailureFeedback(
      spokenText: "",
      passThreshold: passThreshold,
      failureReasonHintKorean: appConfigStore.t('error.timeout_reason'),
    );

    if (!mounted) return;
    _showResultScreen(
      success: false,
      spoken: appConfigStore.t('error.timeout_spoken'),
      isTimeout: true,
    );
  }

  Future<String> _newRecordingPath() async {
    final dir = await getTemporaryDirectory();
    final ts = DateTime.now().millisecondsSinceEpoch;
    if (!kIsWeb && Platform.isAndroid) {
      return '${dir.path}/audio_$ts.m4a';
    }
    return '${dir.path}/audio_$ts.wav';
  }

  void _startRecording() async {
    _timer?.cancel();
    await _cleanupAudioSessions(stopTts: true);
    setState(() {
      _gameState = GameState.recording;
    });

    logEventSafe(
      'record_start',
      parameters: {
        'theme_id': widget.theme.id,
        'level': levelId(widget.level),
        'scenario_id': widget.scenario.id,
        'turn_in_round': _turnInRound,
        'round': _round,
      },
    );

    _answerRecordStartAt = DateTime.now();

    _liveAnswerSttText = "";
    if (!kIsWeb) {
      if (!_sttReady) {
        try {
          _sttReady = await _speechToText.initialize();
        } catch (_) {
          _sttReady = false;
        }
      }
      if (_sttReady) {
        try {
          await _speechToText.listen(
            localeId: 'en_US',
            listenOptions: stt.SpeechListenOptions(partialResults: true),
            onResult: (res) {
              _liveAnswerSttText = res.recognizedWords;
            },
          );
        } catch (_) {
          // ignore
        }
      }
    }

    if (await _audioRecorder.hasPermission()) {
      final path = await _newRecordingPath();
      _activeAnswerRecordPath = path;
      final cfg = (!kIsWeb && Platform.isAndroid)
          ? const RecordConfig(encoder: AudioEncoder.aacLc)
          : const RecordConfig(encoder: AudioEncoder.wav);
      await _audioRecorder.start(cfg, path: path);
    }
  }

  void _stopRecordingAndAnalyze() async {
    setState(() {
      _gameState = GameState.processing;
      _processingMessage = _pickProcessingMessage();
      _userSpokenText = "답변 분석 중... ⏳";
    });

    if (!kIsWeb && _speechToText.isListening) {
      try {
        await _speechToText.stop();
      } catch (_) {
        // ignore
      }
    }

    _processAudioWithGroq(isPractice: false, fallbackText: _liveAnswerSttText);
  }

  void _startPractice() async {
    await _cleanupAudioSessions(stopTts: true);
    final hasMicPermission = await _audioRecorder.hasPermission();
    if (!hasMicPermission) {
      if (!mounted) return;
      setState(() {
        _practiceDebugInfo = 'mic_permission=false';
        _showPracticeDebugInfo = false;
      });
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text(appConfigStore.t('error.mic_permission_needed')),
        ),
      );
      return;
    }

    final practicePath = await _newRecordingPath();
    _activePracticeRecordPath = practicePath;
    _practiceStartAt = DateTime.now();
    _practiceRecognizedText = '';

    setState(() {
      _isPracticing = true;
      _gameState = GameState.result;
      _practiceDebugInfo = 'mic_permission=true path=$practicePath';
      _showPracticeDebugInfo = false;
    });

    try {
      final cfg = (!kIsWeb && Platform.isAndroid)
          ? const RecordConfig(encoder: AudioEncoder.aacLc)
          : const RecordConfig(encoder: AudioEncoder.wav);
      await _audioRecorder.start(cfg, path: practicePath);
    } catch (e) {
      if (!mounted) return;
      setState(() {
        _isPracticing = false;
        _practiceDebugInfo =
            'record_start_exception=$e path=$practicePath perm=$hasMicPermission';
      });
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(const SnackBar(content: Text('녹음 시작에 실패했어요.')));
    }
  }

  void _stopPracticeAndAnalyze() async {
    setState(() {
      _isPracticing = false;
      _gameState = GameState.processing;
      _processingMessage = _pickProcessingMessage();
      _userSpokenText = "연습 발음 분석 중... ⏳";
      _practiceDebugInfo = '';
      _showPracticeDebugInfo = false;
    });

    _appendPracticeDebug(
      'practice_stop start stt_listening=${_speechToText.isListening}',
    );

    if (kIsWeb) {
      await _processAudioWithGroq(isPractice: true);
      return;
    }

    if (_speechToText.isListening) {
      try {
        await _speechToText.stop();
        _appendPracticeDebug('stt.stop ok');
      } catch (_) {
        // ignore
        _appendPracticeDebug('stt.stop exception');
      }
    }

    try {
      await _audioPlayer.stop();
    } catch (_) {
      // ignore
    }

    String? path;
    try {
      if (await _audioRecorder.isRecording()) {
        path = await _audioRecorder.stop();
        _appendPracticeDebug('rec.stop ok path=$path');
      } else {
        _appendPracticeDebug('rec.stop skipped isRecording=false');
      }
    } catch (_) {
      // ignore
      _appendPracticeDebug('rec.stop exception');
    }

    if (path != null && path.trim().isNotEmpty) {
      try {
        final len = await File(path).length();
        debugPrint('[practice_stop] stop_path=$path len=$len');
        _appendPracticeDebug('stop_path_len=$len');
      } catch (_) {
        // ignore
        _appendPracticeDebug('stop_path_len exception');
      }
    }

    final candidates = <String?>[
      path,
      _practiceAudioPath,
      _activePracticeRecordPath,
    ].where((p) => (p ?? '').trim().isNotEmpty).toList();

    String? resolved;
    if (!kIsWeb) {
      for (final c in candidates) {
        if (c == null) continue;
        bool ok = false;
        int len = 0;
        for (int i = 0; i < 8; i++) {
          try {
            ok = await File(c).exists();
            if (ok) {
              len = await File(c).length();
              if (len >= 400) break;
            }
          } catch (_) {
            // ignore
          }
          await Future.delayed(Duration(milliseconds: 120 + (i * 60)));
        }
        if (ok && len >= 400) {
          resolved = c;
          break;
        }
      }
    } else {
      resolved = candidates.isNotEmpty ? candidates.first : null;
    }

    _appendPracticeDebug(
      'resolved_path=${resolved ?? 'null'} candidates=${candidates.length}',
    );

    if (mounted) {
      setState(() {
        final candidate = (resolved != null && resolved.trim().isNotEmpty)
            ? resolved
            : (path != null && path.trim().isNotEmpty)
            ? path
            : null;
        if (candidate != null) {
          _practiceAudioPath = candidate;
        }
      });
    }

    if (_practiceStartAt != null) {
      _practiceAudioDuration = DateTime.now().difference(_practiceStartAt!);
    } else {
      _practiceAudioDuration = Duration.zero;
    }

    final p = _practiceAudioPath;
    if (p == null || p.trim().isEmpty) {
      if (!mounted) return;
      setState(() {
        _practiceDebugInfo =
            'practice_path=null active=$_activePracticeRecordPath';
      });
      _appendPracticeDebug('final_path=null');
      _checkPracticeAnswer(_practiceRecognizedText);
      return;
    }

    if (!kIsWeb) {
      try {
        final exists = await File(p).exists();
        final len = exists ? await File(p).length() : -1;
        _appendPracticeDebug('final_file exists=$exists len=$len');
        if (!exists || len < 400) {
          _appendPracticeDebug('recording_too_small_or_missing');
          _checkPracticeAnswer(_practiceRecognizedText);
          return;
        }
      } catch (e) {
        _appendPracticeDebug('final_file_check_exception=$e');
        _checkPracticeAnswer(_practiceRecognizedText);
        return;
      }
    }

    await _processAudioWithGroq(isPractice: true);
    setState(() {
      _gameState = GameState.processing;
      _processingMessage = _pickProcessingMessage();
    });

    final startedAt = _answerRecordStartAt;
    _answerRecordStartAt = null;
    if (startedAt != null) {
      final d = DateTime.now().difference(startedAt);
      if (d < const Duration(milliseconds: 650)) {
        await _cleanupAudioSessions(stopTts: false);
        if (!mounted) return;
        _showResultScreen(
          success: false,
          spoken: appConfigStore.t('error.empty_speech'),
        );
        return;
      }
    }

    if (!kIsWeb && _speechToText.isListening) {
      try {
        await _speechToText.stop();
      } catch (_) {
        // ignore
      }
    }

    _processAudioWithGroq(isPractice: false, fallbackText: _liveAnswerSttText);
  }

  Future<void> _processAudioWithGroq({
    required bool isPractice,
    String fallbackText = '',
    String? audioPathOverride,
  }) async {
    if (groqApiKey.trim().isEmpty) {
      debugPrint('[Groq_ASR] missing_api_key');
      logEventSafe(
        'asr_fail',
        parameters: {
          'provider': 'groq',
          'reason': 'missing_api_key',
          'is_practice': isPractice ? 1 : 0,
          if (!isPractice) 'theme_id': widget.theme.id,
          if (!isPractice) 'level': levelId(widget.level),
          if (!isPractice) 'scenario_id': widget.scenario.id,
        },
      );
      final fb = fallbackText.trim();
      if (!isPractice && fb.isNotEmpty) {
        await _checkAnswer(_postprocessAsr(fb));
      } else {
        _showResultScreen(
          success: false,
          spoken: 'Groq API 키가 설정되지 않았어요. (ASR 불가)',
        );
      }
      return;
    }

    bool shouldRetryStatus(int status) {
      if (status == 408) return true;
      if (status == 429) return true;
      if (status >= 500 && status <= 599) return true;
      return false;
    }

    final requestStartedAt = DateTime.now();
    final stoppedPath = audioPathOverride ?? await _audioRecorder.stop();
    final path =
        stoppedPath ??
        (isPractice ? _activePracticeRecordPath : _activeAnswerRecordPath);
    if (!isPractice) {
      _lastAnswerAudioPath = path;
    } else {
      _practiceAudioPath = path;
    }
    if (path == null) {
      debugPrint('[Groq_ASR] missing_record_path');
      logEventSafe(
        'asr_fail',
        parameters: {
          'provider': 'groq',
          'reason': 'missing_record_path',
          'is_practice': isPractice ? 1 : 0,
          if (!isPractice) 'theme_id': widget.theme.id,
          if (!isPractice) 'level': levelId(widget.level),
          if (!isPractice) 'scenario_id': widget.scenario.id,
        },
      );
      final fb = fallbackText.trim();
      if (!isPractice && fb.isNotEmpty) {
        await _checkAnswer(_postprocessAsr(fb));
        return;
      }
      if (isPractice && mounted) {
        setState(() {
          _practiceDebugInfo =
              'asr_fail missing_record_path active=$_activePracticeRecordPath';
        });
        _checkPracticeAnswer(fb.isNotEmpty ? _postprocessAsr(fb) : '');
        return;
      }
      _showResultScreen(success: false, spoken: '녹음 파일 오류');
      return;
    }

    if (isPractice) {
      try {
        final len = await File(path).length();
        if (mounted) {
          setState(() {
            _practiceDebugInfo = 'practice_asr_file len=$len path=$path';
          });
        }
      } catch (_) {
        // ignore
      }
    }

    if (isPractice) {
      try {
        final exists = await File(path).exists();
        final len = exists ? await File(path).length() : -1;
        _appendPracticeDebug(
          'groq_req file_exists=$exists len=$len path=$path',
        );
        if (exists && len > 0 && len < 400) {
          _appendPracticeDebug('warning: very_small_audio(len<$len)');
        }
      } catch (e) {
        _appendPracticeDebug('file_check_exception=$e');
      }
    }

    http.Response? response;
    for (int attempt = 0; attempt < 3; attempt++) {
      try {
        final request = http.MultipartRequest(
          'POST',
          Uri.parse('https://api.groq.com/openai/v1/audio/transcriptions'),
        );
        request.headers.addAll({'Authorization': 'Bearer $groqApiKey'});
        request.fields['model'] = 'whisper-large-v3';
        request.fields['language'] = 'en';

        if (kIsWeb) {
          final fileRes = await http
              .get(Uri.parse(path))
              .timeout(const Duration(seconds: 8));
          request.files.add(
            http.MultipartFile.fromBytes(
              'file',
              fileRes.bodyBytes,
              filename: 'audio.webm',
            ),
          );
        } else {
          request.files.add(await http.MultipartFile.fromPath('file', path));
        }

        final streamed = await request.send().timeout(
          Duration(seconds: 18 + (attempt * 6)),
        );
        response = await http.Response.fromStream(streamed);

        if (response.statusCode == 200) break;

        final raw = response.body;
        final clipped = raw.length > 800 ? raw.substring(0, 800) : raw;
        debugPrint(
          '[Groq_ASR] attempt=$attempt status=${response.statusCode} body="$clipped"',
        );

        if (isPractice) {
          _appendPracticeDebug(
            'groq_http status=${response.statusCode} attempt=$attempt body=${clipped.replaceAll("\n", " ")}',
          );
        }

        if (shouldRetryStatus(response.statusCode) && attempt < 2) {
          await Future.delayed(
            Duration(milliseconds: 300 * (attempt + 1) * (attempt + 1)),
          );
          continue;
        }
        break;
      } catch (e) {
        debugPrint('[Groq_ASR] exception attempt=$attempt err=$e');
        if (isPractice) {
          _appendPracticeDebug('groq_exception attempt=$attempt err=$e');
        }
        if (attempt < 2) {
          await Future.delayed(
            Duration(milliseconds: 350 * (attempt + 1) * (attempt + 1)),
          );
          continue;
        }
      }
    }

    if (response == null) {
      logEventSafe(
        'asr_fail',
        parameters: {
          'provider': 'groq',
          'reason': 'no_response',
          'is_practice': isPractice ? 1 : 0,
          if (!isPractice) 'theme_id': widget.theme.id,
          if (!isPractice) 'level': levelId(widget.level),
          if (!isPractice) 'scenario_id': widget.scenario.id,
        },
      );
      final fb = fallbackText.trim();
      if (!isPractice && fb.isNotEmpty) {
        await _checkAnswer(_postprocessAsr(fb));
        return;
      }
      if (isPractice) {
        final ms = DateTime.now().difference(requestStartedAt).inMilliseconds;
        _appendPracticeDebug('groq_no_response elapsed_ms=$ms');
      }
      _showResultScreen(
        success: false,
        spoken: 'ASR 서버 연결 실패(예외). 네트워크를 확인해 주세요.',
      );
      return;
    }

    if (response.statusCode == 200) {
      final data = jsonDecode(response.body);
      final transcribedText = (data['text'] ?? "").toString();
      if (isPractice) {
        final ms = DateTime.now().difference(requestStartedAt).inMilliseconds;
        _appendPracticeDebug(
          'groq_ok elapsed_ms=$ms text_len=${transcribedText.trim().length}',
        );
      }

      if (transcribedText.trim().isEmpty) {
        logEventSafe(
          'asr_fail',
          parameters: {
            'provider': 'groq',
            'reason': 'empty_text_200',
            'is_practice': isPractice ? 1 : 0,
            if (!isPractice) 'theme_id': widget.theme.id,
            if (!isPractice) 'level': levelId(widget.level),
            if (!isPractice) 'scenario_id': widget.scenario.id,
          },
        );
        if (isPractice) {
          final clipped = response.body.length > 600
              ? response.body.substring(0, 600)
              : response.body;
          _appendPracticeDebug(
            'groq_200_empty body=${clipped.replaceAll("\n", " ")}',
          );
          final fb = fallbackText.trim();
          _checkPracticeAnswer(fb.isNotEmpty ? _postprocessAsr(fb) : '');
          return;
        }
      }
      logEventSafe(
        'asr_ok',
        parameters: {
          'provider': 'groq',
          'is_practice': isPractice ? 1 : 0,
          'text_len': transcribedText.trim().length,
          if (!isPractice) 'theme_id': widget.theme.id,
          if (!isPractice) 'level': levelId(widget.level),
          if (!isPractice) 'scenario_id': widget.scenario.id,
        },
      );
      String spoken = transcribedText
          .replaceAll(RegExp(r'[^\w\s]'), '')
          .toLowerCase();

      if (!isPractice) {
        spoken = _postprocessAsr(spoken);
        await _checkAnswer(spoken);
      } else {
        _practiceRecognizedText = spoken;
        if (_practiceStartAt != null) {
          _practiceAudioDuration = DateTime.now().difference(_practiceStartAt!);
        }
        _checkPracticeAnswer(spoken);
      }
      return;
    }

    final fb = fallbackText.trim();
    if (!isPractice && fb.isNotEmpty) {
      await _checkAnswer(_postprocessAsr(fb));
      return;
    }

    final status = response.statusCode;
    logEventSafe(
      'asr_fail',
      parameters: {
        'provider': 'groq',
        'reason': 'http_$status',
        'is_practice': isPractice ? 1 : 0,
        if (!isPractice) 'theme_id': widget.theme.id,
        if (!isPractice) 'level': levelId(widget.level),
        if (!isPractice) 'scenario_id': widget.scenario.id,
      },
    );
    if (isPractice) {
      final raw = response.body;
      final clipped = raw.length > 600 ? raw.substring(0, 600) : raw;
      final ms = DateTime.now().difference(requestStartedAt).inMilliseconds;
      _appendPracticeDebug(
        'groq_fail status=$status elapsed_ms=$ms body=${clipped.replaceAll("\n", " ")}',
      );
      _checkPracticeAnswer(fb.isNotEmpty ? _postprocessAsr(fb) : '');
      return;
    }
    _showResultScreen(
      success: false,
      spoken: 'ASR 실패(Groq $status). 네트워크 상태를 확인해 주세요.',
    );
  }

  String _postprocessAsr(String spoken) {
    final requiredKeywords = _requiredKeywordsForCurrentScenario();
    if (requiredKeywords.isEmpty) return spoken;

    final tokens = spoken
        .split(RegExp(r'\s+'))
        .where((t) => t.trim().isNotEmpty)
        .toList();
    for (int i = 0; i < tokens.length; i++) {
      final t = tokens[i];
      final normalized = t.toLowerCase();

      if (normalized == 'copy' &&
          requiredKeywords.any((k) => k.toLowerCase() == 'coffee')) {
        tokens[i] = 'coffee';
        continue;
      }

      final best = _closestKeyword(normalized, requiredKeywords);
      if (best != null) {
        tokens[i] = best;
      }
    }

    return tokens.join(' ');
  }

  String? _closestKeyword(String token, List<String> keywords) {
    String? best;
    int bestDist = 999;
    for (final k in keywords) {
      final kw = k.toLowerCase();
      if (kw.length < 4 || token.length < 3) continue;
      final d = _levenshtein(token, kw);
      if (d < bestDist) {
        bestDist = d;
        best = kw;
      }
    }

    if (best == null) return null;
    if (bestDist <= 1) return best;
    if (bestDist == 2 && token.length >= 5) return best;
    return null;
  }

  int _levenshtein(String s, String t) {
    final m = s.length;
    final n = t.length;
    if (m == 0) return n;
    if (n == 0) return m;

    final prev = List<int>.generate(n + 1, (j) => j);
    final curr = List<int>.filled(n + 1, 0);

    for (int i = 1; i <= m; i++) {
      curr[0] = i;
      final si = s.codeUnitAt(i - 1);
      for (int j = 1; j <= n; j++) {
        final cost = si == t.codeUnitAt(j - 1) ? 0 : 1;
        final del = prev[j] + 1;
        final ins = curr[j - 1] + 1;
        final sub = prev[j - 1] + cost;
        curr[j] = del < ins ? (del < sub ? del : sub) : (ins < sub ? ins : sub);
      }
      for (int j = 0; j <= n; j++) {
        prev[j] = curr[j];
      }
    }
    return prev[n];
  }

  Future<void> _checkAnswer(String spokenText) async {
    final diag = <String>[];
    diag.add('spoken="${spokenText.replaceAll('\n', ' ').trim()}"');
    diag.add('q="${_currentQuestion.replaceAll('\n', ' ').trim()}"');
    diag.add('revives_used=${_gameProvider.revivesUsedThisTurn}');
    final lower = spokenText.trim().toLowerCase();
    if (lower.isEmpty) {
      final guide = _fallbackGuideForLine(_currentQuestion);
      final meaning = _fallbackMeaningForLine(_currentQuestion);
      setState(() {
        _currentGuide = guide;
        _currentMeaning = meaning;
        _currentTip = _fallbackTipKorean();
        _currentFailReason = appConfigStore.t('error.empty_speech');
        _lastGainedPoints = 0;
        diag.add('path=empty_speech');
        _turnDiag = diag.join(' | ');
      });
      _gameProvider.registerTurnFailure();
      _showResultScreen(
        success: false,
        spoken: appConfigStore.t('error.empty_speech'),
      );
      return;
    }

    // AI 판정 절대주의: 로컬 필터로 성공/실패를 뒤집지 않는다.
    // (침묵/공백은 위에서만 실패 처리)

    final passThreshold = switch (widget.level) {
      SurvivalLevel.beginner => 70,
      SurvivalLevel.intermediate => 80,
      SurvivalLevel.advanced => 90,
      SurvivalLevel.hardcore => 100,
    };

    final judgement = await _ai.judgeAndNext(
      theme: widget.theme,
      level: widget.level,
      passThreshold: passThreshold,
      scenario: widget.scenario,
      history: _recentHistoryForGemini(),
      npcLine: _currentQuestion,
      userAnswer: spokenText,
    );

    if (judgement == null) {
      final client = _ai;
      final dbg = client is FailoverAiClient
          ? (client.lastDebugSummary?.trim() ?? '')
          : '';
      final msg = dbg.isNotEmpty
          ? dbg
          : 'judge=null status=${_ai.lastHttpStatus} ex=${_ai.lastException}';
      diag.add('path=ai_judge_null');
      diag.add(msg.replaceAll('\n', ' '));
      setState(() {
        _currentGuide = _fallbackGuideForLine(_currentQuestion);
        _currentMeaning = _fallbackMeaningForLine(_currentQuestion);
        _currentTip = _decorateTipWithGloss(
          _fallbackTipKorean(),
          idealAnswer: _fallbackGuideForLine(_currentQuestion),
        );
        _currentFailReason = 'AI 판정에 실패했어요. 네트워크/쿼터/응답 형식을 확인해 주세요.';
        _turnDiag = diag.join(' | ');
      });
      _showAiErrorDetailsThrottled(
        title: 'AI 통신 실패 (판정)',
        fallback:
            'judge=null status=${_ai.lastHttpStatus} ex=${_ai.lastException} body=${(_ai.lastHttpBody ?? '').trim()}'
                .trim(),
      );
      _showResultScreen(success: false, spoken: spokenText);
      return;
    }

    if (!mounted) return;

    GeminiTurnJudgement j = _sanitizeJudgement(judgement);

    String safeNpcNext(String candidate, {required bool relevant}) {
      final c = candidate.trim();
      if (c.isNotEmpty && !_isBadNpcLine(c)) return c;
      String fb = relevant
          ? _localFallbackNpcNext(spokenText)
          : _styleNpcByLevel('Sorry, what do you mean exactly?');
      fb = fb.trim();
      if (fb.isNotEmpty && !_isBadNpcLine(fb)) return fb;
      final last = _styleNpcByLevel('Okay, tell me more.').trim();
      return (last.isNotEmpty && !_isBadNpcLine(last))
          ? last
          : _fallbackOpeningLine();
    }

    j = GeminiTurnJudgement(
      contextScore: j.contextScore,
      relevant: j.relevant,
      reasonKorean: j.reasonKorean,
      npcNextLine: safeNpcNext(j.npcNextLine, relevant: j.relevant),
      idealAnswer: j.idealAnswer,
      idealAnswerKorean: j.idealAnswerKorean,
      tipKorean: j.tipKorean,
    );

    if (!_isIdealAnswerCompatible(
      question: _currentQuestion,
      ideal: j.idealAnswer,
    )) {
      j = GeminiTurnJudgement(
        contextScore: j.contextScore,
        relevant: j.relevant,
        reasonKorean: j.reasonKorean,
        npcNextLine: j.npcNextLine,
        idealAnswer: _fallbackGuideForLine(_currentQuestion),
        idealAnswerKorean: _fallbackMeaningForLine(_currentQuestion),
        tipKorean: _fallbackTipKorean(),
      );
    }

    final required = _requiredKeywordsForCurrentScenario();
    if (required.isNotEmpty) {
      final idealLower = j.idealAnswer.toLowerCase();
      final hasRequired = required.any((k) {
        final kk = k.trim().toLowerCase();
        if (kk.isEmpty) return false;
        return idealLower.contains(kk);
      });
      if (!hasRequired) {
        j = GeminiTurnJudgement(
          contextScore: j.contextScore,
          relevant: j.relevant,
          reasonKorean: j.reasonKorean,
          npcNextLine: j.npcNextLine,
          idealAnswer: _fallbackGuideForLine(_currentQuestion),
          idealAnswerKorean: _fallbackMeaningForLine(_currentQuestion),
          tipKorean: _fallbackTipKorean(),
        );
      }
    }

    final safeIdealKo = _isGoodKoreanSentence(j.idealAnswerKorean)
        ? j.idealAnswerKorean
        : _fallbackMeaningForLine(_currentQuestion);
    final sanitizedTip = _sanitizeTipKorean(j.tipKorean);
    final safeTip = sanitizedTip.trim().length >= 6
        ? sanitizedTip
        : _fallbackTipKorean();

    setState(() {
      _currentGuide = j.idealAnswer;
      _currentMeaning = safeIdealKo;
      _currentTip = _decorateTipWithGloss(safeTip, idealAnswer: j.idealAnswer);
      _currentFailReason = j.reasonKorean;
    });

    if (_currentMeaning.trim().isEmpty) {
      setState(() {
        _currentMeaning = _fallbackMeaningForLine(_currentQuestion);
      });
    }
    if (_currentTip.trim().isEmpty) {
      setState(() {
        _currentTip = _decorateTipWithGloss(
          _fallbackTipKorean(),
          idealAnswer: _currentGuide,
        );
      });
    }

    logEventSafe(
      'judge_done',
      parameters: {
        'theme_id': widget.theme.id,
        'level': levelId(widget.level),
        'scenario_id': widget.scenario.id,
        'turn_in_round': _turnInRound,
        'round': _round,
        'relevant': j.relevant ? 1 : 0,
        'context_score': j.contextScore,
        'pass_threshold': passThreshold,
        'hard_non_answer': 0,
        'intent_rescue': 0,
      },
    );

    _backgroundPreload();

    if (!j.relevant) {
      _gameProvider.registerTurnFailure();
      setState(() {
        _canContinueAfterFail = false;
        _continueNextNpcLine = '';
        _lastGainedPoints = 0;
        diag.add('judge=relevant=false score=${j.contextScore}');
        _turnDiag = diag.join(' | ');
      });
      _showResultScreen(success: false, spoken: spokenText);
      return;
    }

    final gained = _gameProvider.gainedPointsForCurrentTurn();
    _gameProvider.registerTurnSuccess();
    _lastGainedPoints = gained;
    _turnInRound++;
    _conversation.add({"role": "user", "text": spokenText});
    _conversation.add({"role": "npc", "text": j.npcNextLine});
    setState(() {
      _canContinueAfterFail = false;
      _continueNextNpcLine = '';
      diag.add('judge=relevant=true score=${j.contextScore} gained=$gained');
      _turnDiag = diag.join(' | ');
    });
    _showResultScreen(success: true, spoken: spokenText);
  }

  List<Map<String, String>> _recentHistoryForGemini() {
    const int maxMessages = 12;
    if (_conversation.length <= maxMessages) {
      return List<Map<String, String>>.from(_conversation);
    }
    return List<Map<String, String>>.from(
      _conversation.sublist(_conversation.length - maxMessages),
    );
  }

  List<String> _requiredKeywordsForCurrentScenario() {
    if (widget.scenario.keywords.isNotEmpty) return widget.scenario.keywords;
    return switch (widget.theme.id) {
      'cafe' => const [
        "coffee",
        "order",
        "wrong",
        "cold",
        "hot",
        "iced",
        "cream",
        "sugar",
        "milk",
        "latte",
        "americano",
      ],
      'airport' => const ["passport", "baggage", "lost", "missing", "visit"],
      'workplace' => const ["status", "update", "deadline", "risk"],
      'neighborhood' => const ["noise", "package", "delivery", "missing"],
      _ => const [],
    };
  }

  void _checkPracticeAnswer(String spokenText) {
    final textScore = _calculateTextSimilarityScorePercent(
      spokenText,
      _currentGuide,
    );
    final fluencyScore = _calculateFluencyScorePercent(
      targetText: _currentGuide,
      actualDuration: _practiceAudioDuration,
    );
    final finalScore = ((textScore * 0.7) + (fluencyScore * 0.3)).round().clamp(
      0,
      100,
    );

    setState(() {
      _isPracticeResult = true;
      _practiceTextScorePercent = textScore;
      _practiceFluencyScorePercent = fluencyScore;
      _practiceScorePercent = finalScore;
      _userSpokenText = spokenText.trim().isEmpty
          ? "(인식된 문장이 없어요)"
          : spokenText;
      _gameState = GameState.result;
    });
  }

  int _calculateTextSimilarityScorePercent(String recognized, String target) {
    String norm(String s) => s
        .toLowerCase()
        .replaceAll(RegExp(r'[^a-z0-9\s]'), ' ')
        .replaceAll(RegExp(r'\s+'), ' ')
        .trim();
    final a = norm(recognized);
    final b = norm(target);
    if (a.isEmpty || b.isEmpty) return 0;
    final dist = _levenshtein(a, b);
    final maxLen = max(a.length, b.length);
    if (maxLen <= 0) return 0;
    final sim = 1.0 - (dist / maxLen);
    return (sim * 100).round().clamp(0, 100);
  }

  int _calculateFluencyScorePercent({
    required String targetText,
    required Duration actualDuration,
  }) {
    final norm = targetText
        .toLowerCase()
        .replaceAll(RegExp(r'[^a-z0-9\s]'), ' ')
        .replaceAll(RegExp(r'\s+'), ' ')
        .trim();
    final words = norm.isEmpty ? 0 : norm.split(' ').length;
    if (words <= 0) return 0;

    final expectedSeconds = (words * 0.45) + 0.4;
    final expectedMs = (expectedSeconds * 1000).round().clamp(1, 600000);
    final expected = Duration(milliseconds: expectedMs);
    if (actualDuration.inMilliseconds <= 0) return 0;
    final actual = actualDuration;
    final ratio = actual.inMilliseconds / expected.inMilliseconds;

    if (ratio >= 0.75 && ratio <= 1.40) return 100;
    if (ratio >= 0.50 && ratio < 0.75) {
      final t = (ratio - 0.50) / (0.75 - 0.50);
      return (60 + (40 * t)).round().clamp(0, 100);
    }
    if (ratio > 1.40 && ratio <= 2.00) {
      final t = (2.00 - ratio) / (2.00 - 1.40);
      return (60 + (40 * t)).round().clamp(0, 100);
    }
    return 30;
  }

  void _showResultScreen({
    required bool success,
    required String spoken,
    bool isTimeout = false,
  }) {
    if (!success && !_isPracticeResult) {
      unawaited(_incrementDeathCount());
    }
    if (!_isPracticeResult) {
      logEventSafe(
        'turn_result',
        parameters: {
          'theme_id': widget.theme.id,
          'level': levelId(widget.level),
          'scenario_id': widget.scenario.id,
          'success': success ? 1 : 0,
          'timeout': isTimeout ? 1 : 0,
          'score': _gameProvider.score,
          'turn_in_round': _turnInRound,
          'round': _round,
        },
      );
    }
    setState(() {
      _gameState = GameState.result;
      _isSuccess = success;
      _userSpokenText = spoken;
    });
  }

  Future<void> _applyFailureFeedback({
    required String spokenText,
    required int passThreshold,
    required String failureReasonHintKorean,
  }) async {
    final feedback = await _ai.generateFailureFeedback(
      theme: widget.theme,
      level: widget.level,
      scenario: widget.scenario,
      npcLine: _currentQuestion,
      userAnswer: spokenText,
      passThreshold: passThreshold,
      failureReasonHintKorean: failureReasonHintKorean,
    );

    if (!mounted) return;

    if (feedback == null) {
      final guide = _fallbackGuideForLine(_currentQuestion);
      final meaning = _fallbackMeaningForLine(_currentQuestion);
      setState(() {
        _currentGuide = _styleCoachingByLevel(guide);
        _currentMeaning = meaning;
        _currentTip = _decorateTipWithGloss(
          _fallbackTipKorean(),
          idealAnswer: _styleCoachingByLevel(guide),
        );
        _currentFailReason = failureReasonHintKorean.trim().isNotEmpty
            ? failureReasonHintKorean.trim()
            : "상황에 필요한 정보가 부족했어요.";
      });
      return;
    }

    setState(() {
      _currentGuide = feedback.idealAnswer;
      _currentMeaning = feedback.idealAnswerKorean;
      _currentTip = _decorateTipWithGloss(
        feedback.tipKorean,
        idealAnswer: feedback.idealAnswer,
      );
      _currentFailReason = feedback.reasonKorean;
    });
  }

  void _handleScreenTap() {
    switch (_gameState) {
      case GameState.ready:
        _startNpcConversation();
        break;
      case GameState.waitingForTap:
        logEventSafe(
          'tap_to_record',
          parameters: {
            'theme_id': widget.theme.id,
            'level': levelId(widget.level),
            'scenario_id': widget.scenario.id,
            'turn_in_round': _turnInRound,
            'round': _round,
          },
        );
        _startRecording();
        break;
      case GameState.recording:
        _stopRecordingAndAnalyze();
        break;
      default:
        break;
    }
  }

  String _rewardedAdUnitIdForRevive() {
    if (kIsWeb) return '';
    if (Platform.isAndroid) return 'ca-app-pub-3940256099942544/5224354917';
    if (Platform.isIOS) return 'ca-app-pub-3940256099942544/1712485313';
    return '';
  }

  Future<void> _showRewardedReviveAdThenContinue() async {
    if (_gameProvider.isGameOver) return;
    if (_gameProvider.revivesUsedThisTurn >= 1) return;
    if (!mounted) return;

    logEventSafe(
      'revive_click',
      parameters: {
        'theme_id': widget.theme.id,
        'level': levelId(widget.level),
        'revives_used': _gameProvider.revivesUsedThisTurn,
      },
    );

    logEventSafe(
      'revive_attempt',
      parameters: {
        'theme_id': widget.theme.id,
        'level': levelId(widget.level),
        'revives_used': _gameProvider.revivesUsedThisTurn,
      },
    );

    final unitId = _rewardedAdUnitIdForRevive();
    if (unitId.trim().isEmpty) {
      debugPrint('[Ad_Revive] no_unit_id fallback_revive');
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('광고 설정이 없어 이어하기를 사용할 수 없어요.'),
            duration: Duration(seconds: 2),
          ),
        );
      }
      return;
    }

    if (_reviveAdLoading) return;
    _reviveAdLoading = true;
    debugPrint('[Ad_Revive] load_start unit=$unitId');

    try {
      await RewardedAd.load(
        adUnitId: unitId,
        request: const AdRequest(),
        rewardedAdLoadCallback: RewardedAdLoadCallback(
          onAdLoaded: (ad) async {
            debugPrint('[Ad_Revive] load_ok');
            logEventSafe(
              'rewarded_loaded',
              parameters: {
                'theme_id': widget.theme.id,
                'level': levelId(widget.level),
              },
            );
            _reviveRewardedAd = ad;
            ad.fullScreenContentCallback = FullScreenContentCallback(
              onAdDismissedFullScreenContent: (ad) {
                debugPrint('[Ad_Revive] dismissed');
                logEventSafe(
                  'rewarded_dismissed',
                  parameters: {
                    'theme_id': widget.theme.id,
                    'level': levelId(widget.level),
                  },
                );
                ad.dispose();
                if (identical(_reviveRewardedAd, ad)) _reviveRewardedAd = null;
              },
              onAdFailedToShowFullScreenContent: (ad, err) {
                debugPrint('[Ad_Revive] show_failed $err');
                logEventSafe(
                  'rewarded_show_failed',
                  parameters: {
                    'theme_id': widget.theme.id,
                    'level': levelId(widget.level),
                    'err': err.toString(),
                  },
                );
                ad.dispose();
                if (identical(_reviveRewardedAd, ad)) _reviveRewardedAd = null;
              },
            );

            final toShow = _reviveRewardedAd;
            _reviveRewardedAd = null;
            _reviveAdLoading = false;
            if (!mounted) {
              toShow?.dispose();
              return;
            }
            if (toShow == null) {
              debugPrint('[Ad_Revive] null_after_load fallback_revive');
              if (mounted) {
                ScaffoldMessenger.of(context).showSnackBar(
                  const SnackBar(
                    content: Text('광고를 보여줄 수 없어요. 잠시 후 다시 시도해 주세요.'),
                    duration: Duration(seconds: 2),
                  ),
                );
              }
              return;
            }

            logEventSafe(
              'rewarded_show',
              parameters: {
                'theme_id': widget.theme.id,
                'level': levelId(widget.level),
              },
            );

            await toShow.show(
              onUserEarnedReward: (ad, reward) {
                debugPrint(
                  '[Ad_Revive] reward type=${reward.type} amount=${reward.amount}',
                );
                logEventSafe(
                  'revive_rewarded',
                  parameters: {
                    'theme_id': widget.theme.id,
                    'level': levelId(widget.level),
                    'reward_type': reward.type,
                    'reward_amount': reward.amount,
                  },
                );
                _reviveAndContinue();
              },
            );
          },
          onAdFailedToLoad: (err) {
            debugPrint('[Ad_Revive] load_failed $err');
            logEventSafe(
              'rewarded_failed',
              parameters: {
                'theme_id': widget.theme.id,
                'level': levelId(widget.level),
                'err': err.toString(),
              },
            );
            _reviveAdLoading = false;
            if (mounted) {
              ScaffoldMessenger.of(context).showSnackBar(
                const SnackBar(
                  content: Text('광고를 불러오지 못했어요. 잠시 후 다시 시도해 주세요.'),
                  duration: Duration(seconds: 2),
                ),
              );
            }
          },
        ),
      );
    } catch (e) {
      debugPrint('[Ad_Revive] load_exception $e');
      _reviveAdLoading = false;
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('광고 오류가 발생했어요. 잠시 후 다시 시도해 주세요.'),
            duration: Duration(seconds: 2),
          ),
        );
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    final showBanner =
        _gameState == GameState.result ||
        _gameState == GameState.waitingForTap ||
        _gameState == GameState.recording;
    final content = Container(
      width: double.infinity,
      height: double.infinity,
      color: Colors.transparent,
      padding: const EdgeInsets.all(24),

      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          Text(
            "$_currentCategory (SCORE: ${_gameProvider.score} · TURN: ${(_round - 1) * 10 + _turnInRound}/ ${_maxRounds * 10})",
            style: const TextStyle(
              color: Colors.cyanAccent,
              fontSize: 18,
              fontWeight: FontWeight.bold,
            ),
          ),
          const SizedBox(height: 30),
          Text(
            _currentQuestion,
            style: const TextStyle(
              color: Colors.white,
              fontSize: 32,
              fontWeight: FontWeight.w900,
            ),
            textAlign: TextAlign.center,
          ),
          if (_gameState == GameState.result &&
              _currentQuestionKorean.trim().isNotEmpty) ...[
            const SizedBox(height: 10),
            Text(
              _currentQuestionKorean,
              style: TextStyle(
                color: Colors.white.withValues(alpha: 0.75),
                fontSize: 18,
                fontWeight: FontWeight.w600,
              ),
              textAlign: TextAlign.center,
            ),
          ],
          const SizedBox(height: 24),
          Expanded(
            child: Center(
              child: SingleChildScrollView(
                physics: _gameState == GameState.result
                    ? const BouncingScrollPhysics()
                    : const NeverScrollableScrollPhysics(),
                child: _buildCenterUI(),
              ),
            ),
          ),
        ],
      ),
    );

    final needsScreenTap =
        _gameState == GameState.ready ||
        _gameState == GameState.waitingForTap ||
        _gameState == GameState.recording;

    return Scaffold(
      backgroundColor: const Color(0xFF0A0A0A),
      bottomNavigationBar: (showBanner && _bannerReady && _banner != null)
          ? SizedBox(
              height: _banner!.size.height.toDouble(),
              width: double.infinity,
              child: AdWidget(key: ValueKey(_banner.hashCode), ad: _banner!),
            )
          : null,
      body: needsScreenTap
          ? GestureDetector(onTap: _handleScreenTap, child: content)
          : content,
    );
  }

  Widget _buildCenterUI() {
    if (_gameState == GameState.ready) {
      return const Text(
        "화면을 탭하여 시작!",
        style: TextStyle(
          color: Colors.yellow,
          fontSize: 24,
          fontWeight: FontWeight.bold,
        ),
      );
    }
    if (_gameState == GameState.speakingQuestion) {
      return const Column(
        children: [
          CircularProgressIndicator(color: Colors.white30),
          SizedBox(height: 20),
          Text(
            "원어민 목소리 튜닝 중...",
            style: TextStyle(color: Colors.grey, fontSize: 18),
          ),
        ],
      );
    }
    if (_gameState == GameState.waitingForTap) {
      return Column(
        children: [
          const Text(
            "지금 탭하여 대답하세요!",
            style: TextStyle(color: Colors.white, fontSize: 20),
          ),
          const SizedBox(height: 20),
          Text(
            '$_counter',
            style: TextStyle(
              color: _counter <= 1 ? Colors.redAccent : Colors.red,
              fontSize: 130,
              fontWeight: FontWeight.bold,
              shadows: const [Shadow(blurRadius: 20, color: Colors.red)],
            ),
          ),
        ],
      );
    }
    if (_gameState == GameState.recording) {
      return const Column(
        children: [
          Icon(Icons.mic, color: Colors.redAccent, size: 80),
          SizedBox(height: 10),
          Text(
            "말하는 중... (다 말하면 화면 탭!)",
            style: TextStyle(color: Colors.white70, fontSize: 18),
          ),
        ],
      );
    }
    if (_gameState == GameState.processing) {
      return Column(
        children: [
          const CircularProgressIndicator(color: Colors.cyanAccent),
          const SizedBox(height: 20),
          Text(
            _userSpokenText.contains("연습")
                ? _userSpokenText
                : _processingMessage,
            style: const TextStyle(color: Colors.cyanAccent, fontSize: 18),
          ),
        ],
      );
    }

    if (_gameState != GameState.result) return const SizedBox();

    return Container(
      padding: const EdgeInsets.all(20),
      decoration: BoxDecoration(
        color: Colors.grey.withValues(alpha: 0.1),
        borderRadius: BorderRadius.circular(20),
      ),
      child: Column(
        children: [
          Text(
            _isPracticeResult
                ? "발음 연습 결과"
                : (_isSuccess
                      ? "생존 성공"
                      : (_gameProvider.isGameOver ? "Game Over" : "실패")),
            style: TextStyle(
              color: _isPracticeResult
                  ? Colors.cyanAccent
                  : (_isSuccess ? Colors.greenAccent : Colors.redAccent),
              fontSize: 32,
              fontWeight: FontWeight.bold,
            ),
          ),
          if (!_isPracticeResult && _isSuccess) ...[
            const SizedBox(height: 8),
            Text(
              '+$_lastGainedPoints',
              style: const TextStyle(
                color: Colors.cyanAccent,
                fontSize: 22,
                fontWeight: FontWeight.w900,
              ),
            ),
          ],
          if (!_isSuccess && _currentFailReason.trim().isNotEmpty) ...[
            const SizedBox(height: 10),
            Text(
              _currentFailReason,
              style: const TextStyle(
                color: Colors.white70,
                fontSize: 14,
                fontWeight: FontWeight.w600,
              ),
              textAlign: TextAlign.center,
            ),
          ],
          const SizedBox(height: 20),
          const Text(
            "당신의 대답:",
            style: TextStyle(color: Colors.white54, fontSize: 14),
          ),
          Text(
            '"$_userSpokenText"',
            style: const TextStyle(
              color: Colors.white,
              fontSize: 20,
              fontStyle: FontStyle.italic,
            ),
            textAlign: TextAlign.center,
          ),
          if (kDebugMode && _turnDiag.trim().isNotEmpty) ...[
            const SizedBox(height: 10),
            Container(
              width: double.infinity,
              padding: const EdgeInsets.all(10),
              decoration: BoxDecoration(
                color: Colors.black.withValues(alpha: 0.22),
                borderRadius: BorderRadius.circular(12),
                border: Border.all(color: Colors.white.withValues(alpha: 0.12)),
              ),
              child: Text(
                _turnDiag,
                style: TextStyle(
                  color: Colors.white.withValues(alpha: 0.70),
                  fontSize: 12,
                  height: 1.25,
                ),
              ),
            ),
          ],
          const SizedBox(height: 10),
          if (_isPracticeResult)
            Text(
              "따라 말하기 유사도(ASR 기준): $_practiceScorePercent%",
              style: TextStyle(
                color: _practiceScorePercent >= 70
                    ? Colors.cyanAccent
                    : Colors.orange,
                fontSize: 18,
                fontWeight: FontWeight.bold,
              ),
              textAlign: TextAlign.center,
            ),
          if (_isPracticeResult) ...[
            const SizedBox(height: 6),
            Text(
              "문장 일치도: $_practiceTextScorePercent% · 속도/유창성: $_practiceFluencyScorePercent%",
              style: TextStyle(
                color: Colors.white.withValues(alpha: 0.7),
                fontSize: 13,
                fontWeight: FontWeight.w600,
              ),
              textAlign: TextAlign.center,
            ),
            if (_practiceDebugInfo.trim().isNotEmpty) ...[
              const SizedBox(height: 8),
              TextButton(
                onPressed: () {
                  setState(() {
                    _showPracticeDebugInfo = !_showPracticeDebugInfo;
                  });
                },
                child: Text(
                  _showPracticeDebugInfo ? '디버그 숨기기' : '디버그 보기',
                  style: TextStyle(
                    color: Colors.white.withValues(alpha: 0.65),
                    fontSize: 12,
                    fontWeight: FontWeight.w600,
                  ),
                ),
              ),
              if (_showPracticeDebugInfo ||
                  _userSpokenText.trim() == "(인식된 문장이 없어요)" ||
                  _userSpokenText.trim() == "(인식된 문장이 없어요).") ...[
                Text(
                  _practiceDebugInfo,
                  style: TextStyle(
                    color: Colors.white.withValues(alpha: 0.55),
                    fontSize: 12,
                    fontWeight: FontWeight.w500,
                  ),
                  textAlign: TextAlign.center,
                ),
              ],
            ],
          ],
          const SizedBox(height: 30),
          Text(
            _isPracticeResult ? "연습 문장" : "모범 답안",
            style: const TextStyle(color: Colors.yellow, fontSize: 16),
          ),
          Text(
            _currentGuide,
            style: const TextStyle(
              color: Colors.white,
              fontSize: 24,
              fontWeight: FontWeight.bold,
            ),
            textAlign: TextAlign.center,
          ),
          Text(
            _currentMeaning,
            style: const TextStyle(color: Colors.white54, fontSize: 16),
          ),
          if (_currentTip.trim().isNotEmpty) ...[
            const SizedBox(height: 12),
            const Text(
              "이렇게 말하면 더 좋아요!",
              style: TextStyle(
                color: Colors.cyanAccent,
                fontSize: 15,
                fontWeight: FontWeight.bold,
              ),
            ),
            Text(
              _currentTip,
              style: const TextStyle(color: Colors.white54, fontSize: 14),
              textAlign: TextAlign.center,
            ),
          ],
          const SizedBox(height: 20),
          Wrap(
            alignment: WrapAlignment.center,
            spacing: 10,
            runSpacing: 10,
            children: [
              ElevatedButton(
                onPressed: _returnToLobby,
                style: ElevatedButton.styleFrom(
                  backgroundColor: Colors.black54,
                  padding: const EdgeInsets.symmetric(
                    horizontal: 18,
                    vertical: 14,
                  ),
                ),
                child: const Text(
                  '로비로',
                  style: TextStyle(
                    color: Colors.white,
                    fontSize: 16,
                    fontWeight: FontWeight.bold,
                  ),
                ),
              ),
              ElevatedButton.icon(
                onPressed: () {
                  _speakText(_currentQuestion);
                },
                icon: const Icon(
                  Icons.volume_up,
                  color: Colors.black,
                  size: 20,
                ),
                label: const Text(
                  "듣기",
                  style: TextStyle(
                    color: Colors.black,
                    fontWeight: FontWeight.bold,
                  ),
                ),
                style: ElevatedButton.styleFrom(backgroundColor: Colors.yellow),
              ),
              ElevatedButton.icon(
                onPressed: _currentGuide.trim().isEmpty
                    ? null
                    : () => _speakText(_currentGuide),
                icon: const Icon(
                  Icons.volume_up,
                  color: Colors.black,
                  size: 20,
                ),
                label: const Text(
                  "듣기",
                  style: TextStyle(
                    color: Colors.black,
                    fontWeight: FontWeight.bold,
                  ),
                ),
                style: ElevatedButton.styleFrom(backgroundColor: Colors.yellow),
              ),
              ElevatedButton.icon(
                onPressed: _isPracticing
                    ? _stopPracticeAndAnalyze
                    : _startPractice,
                icon: Icon(
                  _isPracticing ? Icons.stop : Icons.mic,
                  color: Colors.white,
                  size: 20,
                ),
                label: Text(
                  _isPracticing ? "녹음 종료" : "내 발음 연습",
                  style: const TextStyle(
                    color: Colors.white,
                    fontWeight: FontWeight.bold,
                  ),
                ),
                style: ElevatedButton.styleFrom(
                  backgroundColor: _isPracticing
                      ? Colors.redAccent
                      : Colors.blueAccent,
                ),
              ),
              if (_isPracticeResult)
                ElevatedButton.icon(
                  onPressed: (kIsWeb || _practiceAudioPath == null)
                      ? null
                      : () async {
                          await _playLocalAudioFile(
                            _practiceAudioPath!,
                            label: '내 발음 다시 듣기',
                          );
                        },
                  icon: const Icon(
                    Icons.headphones,
                    color: Colors.black,
                    size: 20,
                  ),
                  label: const Text(
                    "🎙️ 내 발음 다시 듣기",
                    style: TextStyle(
                      color: Colors.black,
                      fontWeight: FontWeight.bold,
                    ),
                  ),
                  style: ElevatedButton.styleFrom(
                    backgroundColor: Colors.white,
                  ),
                ),
              ElevatedButton.icon(
                onPressed: _lastAnswerAudioPath == null
                    ? null
                    : () async {
                        try {
                          final path = _lastAnswerAudioPath!;
                          final exists = await File(path).exists();
                          if (!exists) {
                            debugPrint(
                              '[play_answer_audio] missing file path=$path',
                            );
                            if (!mounted) return;
                            ScaffoldMessenger.of(context).showSnackBar(
                              SnackBar(content: Text('녹음 파일이 없어요: $path')),
                            );
                            return;
                          }
                          await _audioPlayer.stop();
                          await _audioPlayer.play(DeviceFileSource(path));
                        } catch (e) {
                          debugPrint('[play_answer_audio] err=$e');
                          if (!mounted) return;
                          ScaffoldMessenger.of(
                            context,
                          ).showSnackBar(SnackBar(content: Text('재생 실패: $e')));
                        }
                      },
                icon: const Icon(
                  Icons.record_voice_over,
                  color: Colors.black,
                  size: 20,
                ),
                label: const Text(
                  "내 답변 다시 듣기",
                  style: TextStyle(
                    color: Colors.black,
                    fontWeight: FontWeight.bold,
                  ),
                ),
                style: ElevatedButton.styleFrom(backgroundColor: Colors.white),
              ),
            ],
          ),
          const SizedBox(height: 30),
          if (_isSuccess)
            ElevatedButton(
              onPressed: () {
                if (_turnInRound >= 10) {
                  _onRoundComplete();
                  return;
                }

                final nextNpcLine = _conversation.isNotEmpty
                    ? _conversation.lastWhere(
                            (e) => e['role'] == 'npc',
                            orElse: () => {'text': ''},
                          )['text'] ??
                          ''
                    : '';
                setState(() {
                  _userSpokenText = "";
                  _isPracticeResult = false;
                  _canContinueAfterFail = false;
                  _continueNextNpcLine = '';
                });
                unawaited(_setQuestionAndSpeak(nextNpcLine, newTurn: true));
              },
              style: ElevatedButton.styleFrom(
                backgroundColor: Colors.green,
                padding: const EdgeInsets.symmetric(
                  horizontal: 40,
                  vertical: 15,
                ),
              ),
              child: Text(
                _turnInRound >= 10 ? "10턴 클리어! 다음 선택" : "다음 턴 계속하기 ",
                style: const TextStyle(
                  color: Colors.white,
                  fontSize: 18,
                  fontWeight: FontWeight.bold,
                ),
              ),
            )
          else
            Wrap(
              alignment: WrapAlignment.center,
              spacing: 12,
              runSpacing: 12,
              children: [
                if (_canContinueAfterFail)
                  ElevatedButton(
                    onPressed: () {
                      final nextLine = _continueNextNpcLine.trim().isNotEmpty
                          ? _continueNextNpcLine
                          : _rescueQuestionAfterNonAnswer();
                      setState(() {
                        _userSpokenText = '';
                        _isPracticeResult = false;
                        _canContinueAfterFail = false;
                        _continueNextNpcLine = '';
                      });
                      unawaited(_setQuestionAndSpeak(nextLine, newTurn: true));
                    },
                    style: ElevatedButton.styleFrom(
                      backgroundColor: Colors.blueGrey,
                      padding: const EdgeInsets.symmetric(
                        horizontal: 18,
                        vertical: 14,
                      ),
                    ),
                    child: const Text(
                      '0점으로 다음 턴 계속하기',
                      style: TextStyle(
                        color: Colors.white,
                        fontSize: 16,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                  ),
                if (!_gameProvider.isGameOver &&
                    _gameProvider.revivesUsedThisTurn < 1)
                  ElevatedButton(
                    onPressed: _showRewardedReviveAdThenContinue,
                    style: ElevatedButton.styleFrom(
                      backgroundColor: Colors.orange,
                      padding: const EdgeInsets.symmetric(
                        horizontal: 18,
                        vertical: 14,
                      ),
                    ),
                    child: Text(
                      "이어하기 (${_gameProvider.revivesUsedThisTurn + 1}/1)",
                      style: const TextStyle(
                        color: Colors.black,
                        fontSize: 16,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                  ),
                ElevatedButton(
                  onPressed: () {
                    unawaited(() async {
                      var pool = contentStore.getScenarios(
                        widget.theme.id,
                        widget.level,
                      );

                      if (pool.isEmpty) {
                        bool dialogShown = false;
                        if (mounted) {
                          dialogShown = true;
                          showDialog<void>(
                            context: context,
                            barrierDismissible: false,
                            builder: (_) {
                              return const AlertDialog(
                                content: Row(
                                  children: [
                                    SizedBox(
                                      width: 22,
                                      height: 22,
                                      child: CircularProgressIndicator(),
                                    ),
                                    SizedBox(width: 12),
                                    Expanded(child: Text('새로운 상황을 준비 중입니다...')),
                                  ],
                                ),
                              );
                            },
                          );
                        }

                        unawaited(
                          contentStore.ensureScenarioCache(
                            themeId: widget.theme.id,
                            level: widget.level,
                            minCount: 10,
                          ),
                        );

                        final start = DateTime.now();
                        while (DateTime.now().difference(start) <
                            const Duration(seconds: 10)) {
                          await Future.delayed(
                            const Duration(milliseconds: 650),
                          );
                          pool = contentStore.getScenarios(
                            widget.theme.id,
                            widget.level,
                          );
                          if (pool.isNotEmpty) break;
                        }

                        if (dialogShown && mounted) {
                          Navigator.of(context, rootNavigator: true).pop();
                        }
                      }

                      pool = contentStore.getScenarios(
                        widget.theme.id,
                        widget.level,
                      );
                      if (pool.isEmpty) {
                        if (mounted) {
                          ScaffoldMessenger.of(context).showSnackBar(
                            const SnackBar(
                              content: Text('통신이 원활하지 않습니다. 로비로 돌아갑니다.'),
                            ),
                          );
                        }
                        await _returnToLobby();
                        return;
                      }

                      // pool has data now, continue to pick next scenario.

                      Scenario newScenario;
                      if (_nextScenarioQueue.isNotEmpty) {
                        newScenario = _nextScenarioQueue.removeAt(0);
                      } else {
                        final recent = _recentScenarioIdsSet(keep: 12)
                          ..add(widget.scenario.id);
                        final filtered = pool
                            .where((s) => !recent.contains(s.id))
                            .toList();
                        final candidates = filtered.isNotEmpty
                            ? filtered
                            : pool;
                        newScenario = candidates.length >= 2
                            ? _pickRandomScenario(
                                candidates,
                                excludeId: widget.scenario.id,
                                avoidIds: recent,
                              )
                            : candidates.first;
                      }

                      debugPrint(
                        "[restart_from_scratch] prev=${widget.scenario.id} next=${newScenario.id} pool=${pool.length}",
                      );
                      logEventSafe(
                        'restart_from_scratch',
                        parameters: {
                          'theme_id': widget.theme.id,
                          'level': levelId(widget.level),
                          'prev_scenario': widget.scenario.id,
                          'next_scenario': newScenario.id,
                          'pool': pool.length,
                        },
                      );

                      if (!mounted) return;
                      Navigator.of(context).pushReplacement(
                        MaterialPageRoute(
                          builder: (_) => GamePage(
                            theme: widget.theme,
                            level: widget.level,
                            scenario: newScenario,
                          ),
                        ),
                      );
                    }());
                  },
                  style: ElevatedButton.styleFrom(
                    backgroundColor: Colors.redAccent,
                    padding: const EdgeInsets.symmetric(
                      horizontal: 18,
                      vertical: 14,
                    ),
                  ),
                  child: const Text(
                    "처음부터",
                    style: TextStyle(
                      color: Colors.white,
                      fontSize: 16,
                      fontWeight: FontWeight.bold,
                    ),
                  ),
                ),
              ],
            ),
        ],
      ),
    );
  }

  void _reviveAndContinue() {
    if (_gameProvider.isGameOver) return;
    final ok = _gameProvider.tryUseRevive();
    if (!ok) return;
    setState(() {
      _userSpokenText = "";
      _isPracticeResult = false;
      _canContinueAfterFail = false;
      _continueNextNpcLine = '';
    });
    unawaited(_setQuestionAndSpeak(_currentQuestion));
  }

  Future<void> _onRoundComplete() async {
    if (!mounted) return;
    final maxReached = _round >= _maxRounds;

    await showDialog<void>(
      context: context,
      barrierDismissible: false,
      builder: (ctx) {
        return AlertDialog(
          title: Text(maxReached ? "리그 종료" : "연장전 선택"),
          content: Text(
            maxReached ? "최대 턴에 도달했습니다." : "그만두기(보상) vs 계속 도전(고득점)",
          ),
          actions: [
            TextButton(
              onPressed: () {
                Navigator.of(ctx).pop();
                unawaited(_returnToLobby());
              },
              child: const Text("그만두기"),
            ),
            if (!maxReached)
              ElevatedButton(
                onPressed: () {
                  Navigator.of(ctx).pop();
                  setState(() {
                    _round++;
                    _turnInRound = 0;
                    _gameState = GameState.speakingQuestion;
                    _currentQuestion =
                        _conversation.lastWhere(
                          (e) => e['role'] == 'npc',
                          orElse: () => {'text': _fallbackOpeningLine()},
                        )['text'] ??
                        _fallbackOpeningLine();
                  });
                  _speakText(_currentQuestion);
                },
                child: const Text("계속 도전"),
              ),
          ],
        );
      },
    );
  }
}
