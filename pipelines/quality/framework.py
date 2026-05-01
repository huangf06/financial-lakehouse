"""Declarative quality framework."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import array, array_compact, expr, lit, struct, when

Severity = Literal["error", "warning"]
Predicate = Callable[[DataFrame], Column]


@dataclass(frozen=True)
class Rule:
    name: str
    error_code: str
    severity: Severity
    description: str
    predicate: Predicate


class QualityFramework:
    def __init__(self, rules: list[Rule]) -> None:
        self._rules = list(rules)

    @property
    def rules(self) -> list[Rule]:
        return list(self._rules)

    def evaluate(self, df: DataFrame) -> DataFrame:
        empty_type = "array<struct<error_code:string,error_msg:string,severity:string>>"
        if not self._rules:
            return df.withColumn("_quality_failures", array().cast(empty_type))

        failure_exprs: list[Column] = []
        for rule in self._rules:
            passes = rule.predicate(df)
            failure = struct(
                lit(rule.error_code).alias("error_code"),
                lit(rule.description).alias("error_msg"),
                lit(rule.severity).alias("severity"),
            )
            failure_exprs.append(when(~passes | passes.isNull(), failure).otherwise(lit(None)))
        return df.withColumn("_quality_failures", array_compact(array(*failure_exprs)))

    def split(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        evaluated = self.evaluate(df)
        has_error = expr("EXISTS(_quality_failures, x -> x.severity = 'error')")
        return evaluated.filter(~has_error).drop("_quality_failures"), evaluated.filter(has_error)
