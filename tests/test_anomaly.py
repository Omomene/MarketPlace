def compute_drop(today, avg):
    return (avg - today) / avg


def test_anomaly_detection_triggered():
    today = 70
    avg_7d = 100

    drop = compute_drop(today, avg_7d)

    assert drop == 0.30
    assert drop >= 0.30 


def test_anomaly_not_triggered():
    today = 90
    avg_7d = 100

    drop = compute_drop(today, avg_7d)

    assert drop == 0.10
    assert drop < 0.30


def test_no_division_by_zero():
    today = 50
    avg_7d = 0

    if avg_7d == 0:
        drop = None
    else:
        drop = compute_drop(today, avg_7d)

    assert drop is None