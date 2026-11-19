APPROVE = "APPROVE"
REVIEW = "REVIEW"
BLOCK = "BLOCK"

ML_WEIGHT = 0.85
ANOMALY_WEIGHT = 0.15


def risk_score(rule_score: float, ml_score: float, anomaly_score: float) -> float:
    blended = ML_WEIGHT * ml_score + ANOMALY_WEIGHT * anomaly_score
    return round(min(max(rule_score, blended), 1.0), 4)


def decide(risk: float, review_threshold: float, block_threshold: float) -> str:
    if risk >= block_threshold:
        return BLOCK
    if risk >= review_threshold:
        return REVIEW
    return APPROVE
