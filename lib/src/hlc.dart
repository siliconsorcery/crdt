import 'dart:math';

const _shift = 16;
const _maxCounter = 0xFFFF;
const _maxDrift = 60000; // 1 minute in ms

/// A Hybrid Logical Clock implementation.
/// This class trades time precision for a guaranteed monotonically increasing
/// clock in distributed systems.
/// Inspiration: https://cse.buffalo.edu/tech-reports/2014-04.pdf
class Hlc<T> implements Comparable<Hlc> {
  final int millis;
  final int counter;
  final T nodeId;

  int get logicalTime => (millis << _shift) + counter;

  Hlc(int millis, this.counter, this.nodeId)
      : assert(counter <= _maxCounter),
        assert(nodeId is Comparable),
        assert(nodeId != null),
        // Detect microseconds and convert to millis
        millis = millis < 0x0001000000000000 ? millis : millis ~/ 1000;

  Hlc.zero(T nodeId) : this(0, 0, nodeId);

  Hlc copyWith({int? millis, int? counter, T? nodeId}) =>
      Hlc(millis ?? this.millis, counter ?? this.counter, nodeId ?? this.nodeId);

  Hlc apply({int? millis, int? counter, T? nodeId}) =>
      Hlc(millis ?? this.millis, counter ?? this.counter, nodeId ?? this.nodeId);

  Hlc.fromDate(DateTime dateTime, T nodeId) : this(dateTime.millisecondsSinceEpoch, 0, nodeId);

  Hlc.now(T nodeId) : this.fromDate(DateTime.now(), nodeId);

  Hlc.fromLogicalTime(logicalTime, T nodeId) : this(logicalTime >> _shift, logicalTime & _maxCounter, nodeId);

  factory Hlc.parse(String timestamp, [T Function(String value)? idDecoder]) {
    final counterDash = timestamp.indexOf('-', timestamp.lastIndexOf(':'));
    final nodeIdDash = timestamp.indexOf('-', counterDash + 1);
    final millis = DateTime.parse(timestamp.substring(0, counterDash)).millisecondsSinceEpoch;
    final counter = int.parse(timestamp.substring(counterDash + 1, nodeIdDash), radix: 16);
    final nodeId = timestamp.substring(nodeIdDash + 1);
    return Hlc(millis, counter, idDecoder != null ? idDecoder(nodeId) : nodeId as T);
  }

  /// Generates a unique, monotonic timestamp suitable for transmission to
  /// another system in string format. Local wall time will be used if
  /// [millis] isn't supplied.
  factory Hlc.send(Hlc<T> canonical, {int? millis}) {
    // Retrieve the local wall time if millis is null
    millis = millis ?? DateTime.now().millisecondsSinceEpoch;

    // Unpack the canonical time and counter
    final millisOld = canonical.millis;
    final counterOld = canonical.counter;

    // Calculate the next time and counter
    // * ensure that the logical time never goes backward
    // * increment the counter if time does not advance
    final millisNew = max(millisOld, millis);
    final counterNew = millisOld == millisNew ? counterOld + 1 : 0;

    // Check the result for drift and counter overflow
    if (millisNew - millis > _maxDrift) {
      throw ClockDriftException(millisNew, millis);
    }
    if (counterNew > _maxCounter) {
      throw OverflowException(counterNew);
    }

    return Hlc(millisNew, counterNew, canonical.nodeId);
  }

  /// Compares and validates a timestamp from a remote system with the local
  /// canonical timestamp to preserve monotonicity.
  /// Returns an updated canonical timestamp instance.
  /// Local wall time will be used if [millis] isn't supplied.
  factory Hlc.recv(Hlc<T> canonical, Hlc<T> remote, {int? millis}) {
    // Retrieve the local wall time if millis is null
    millis = millis ?? DateTime.now().millisecondsSinceEpoch;

    // No need to do any more work if the remote logical time is lower
    if (canonical.logicalTime >= remote.logicalTime) return canonical;

    // Assert the node id
    if (canonical.nodeId == remote.nodeId) {
      throw DuplicateNodeException(canonical.nodeId.toString());
    }
    // Assert the remote clock drift
    if (remote.millis - millis > _maxDrift) {
      throw ClockDriftException(remote.millis, millis);
    }

    return Hlc<T>.fromLogicalTime(remote.logicalTime, canonical.nodeId);
  }

  String toJson() => toString();

  @override
  String toString() => '${DateTime.fromMillisecondsSinceEpoch(millis, isUtc: true).toIso8601String()}'
      '-${counter.toRadixString(16).toUpperCase().padLeft(4, '0')}'
      '-$nodeId';

  // `feat(hulc): pack: Pack HULC into a string
  // 10 character base36 encoded millis
  // 4 character base36 encoded counter
  // 1+ charcters nodeId (ideally: 10 character base36 encoded random id)
  String pack() {
    final buffer = StringBuffer();
    buffer.write(millis.toRadixString(36).padLeft(10, '0').substring(0, 10));
    // buffer.write(delimiter);
    buffer.write(counter.toRadixString(36).padLeft(4, '0').substring(0, 4));
    // buffer.write(delimiter);
    buffer.write(nodeId);
    return buffer.toString();
  }

  // `feat(hulc): unpack: Reinflates a HULC from a packed String
  static Hlc unpack(String packed) {
    return Hlc(
      int.parse(packed.substring(0, 10), radix: 36),
      int.parse(packed.substring(10, 14), radix: 36),
      packed.substring(14),
    );
  }

  // `feat(hulc): static makeNodeId
  /// NodeId represented as 10 digit base36 random number as a String.
  /// One in 2,821,109,907,455 chance of getting a duplicate.
  static String randomNodeId() {
    final random = Random.secure();

    final seedA = random.nextInt(4294967296).toRadixString(36);
    final seedB = random.nextInt(4294967296).toRadixString(36);

    final nodeId = (seedA + seedB).padLeft(10, '0').substring(0, 10);

    return nodeId;
  }

  @override
  int get hashCode => toString().hashCode;

  @override
  bool operator ==(other) => other is Hlc<T> && compareTo(other) == 0;

  bool operator <(other) => other is Hlc<T> && compareTo(other) < 0;

  bool operator <=(other) => this < other || this == other;

  bool operator >(other) => other is Hlc<T> && compareTo(other) > 0;

  bool operator >=(other) => this > other || this == other;

  @override
  int compareTo(Hlc other) {
    final time = logicalTime.compareTo(other.logicalTime);
    return time != 0 ? time : (nodeId as Comparable).compareTo(other.nodeId);
  }
}

class ClockDriftException implements Exception {
  final int drift;

  ClockDriftException(int millisTs, int millisWall) : drift = millisTs - millisWall;

  @override
  String toString() => 'Clock drift of $drift ms exceeds maximum ($_maxDrift)';
}

class OverflowException implements Exception {
  final int counter;

  OverflowException(this.counter);

  @override
  String toString() => 'Timestamp counter overflow: $counter';
}

class DuplicateNodeException implements Exception {
  final String nodeId;

  DuplicateNodeException(this.nodeId);

  @override
  String toString() => 'Duplicate node: $nodeId';
}
