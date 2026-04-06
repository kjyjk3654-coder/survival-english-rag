class GameProvider {
  int _score = 0;
  int _revivesUsedThisTurn = 0;
  int _failTurnsInRun = 0;

  int get score => _score;
  int get revivesUsedThisTurn => _revivesUsedThisTurn;
  int get failTurnsInRun => _failTurnsInRun;
  bool get isGameOver => _failTurnsInRun >= 4;

  void resetRun() {
    _score = 0;
    _revivesUsedThisTurn = 0;
    _failTurnsInRun = 0;
  }

  void startNewTurn() {
    _revivesUsedThisTurn = 0;
  }

  bool tryUseRevive() {
    if (_revivesUsedThisTurn >= 1) return false;
    _revivesUsedThisTurn++;
    return true;
  }

  int gainedPointsForCurrentTurn() {
    final gained = 10 - _revivesUsedThisTurn;
    if (gained < 7) return 7;
    if (gained > 10) return 10;
    return gained;
  }

  void registerTurnSuccess() {
    _score += gainedPointsForCurrentTurn();
    _revivesUsedThisTurn = 0;
  }

  void registerTurnFailure() {
    _failTurnsInRun++;
    _revivesUsedThisTurn = 0;
  }
}
