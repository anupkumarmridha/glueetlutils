from awsgluedq.transforms import EvaluateDataQuality
from awsglue.transforms import SelectFromCollection
from glueetlutils.logger import logger
from glueetlutils.timing import log_time


class QualityUtils:
    def __init__(self, glue_context=None):
        """
        Initialize QualityUtils with an optional Glue context.

        :param glue_context: (optional) GlueContext to use for Glue-specific operations.
        """
        self.glue_context = glue_context

    @log_time
    def evaluate_data_quality(
        self,
        dynamic_frame,
        ruleset,
        context_name,
        publishing_options=None,
        additional_options=None,
    ):
        """
        Evaluate data quality for the given DynamicFrame using the specified ruleset.

        :param dynamic_frame: The DynamicFrame to evaluate.
        :param ruleset: The data quality ruleset to apply (JSON or path).
        :param context_name: A unique string identifying the context for logging and publishing.
        :param publishing_options: Optional dictionary for data quality result publishing options.
        :param additional_options: Optional dictionary for additional configurations.
        :return: A summary of data quality evaluation as a dictionary.
        """
        try:
            logger.info(f"Evaluating data quality for context: {context_name}...")

            # Set default publishing options
            publishing_options = publishing_options or {
                "dataQualityEvaluationContext": context_name,
                "enableDataQualityCloudWatchMetrics": True,
                "enableDataQualityResultsPublishing": True,
            }

            # Set default additional options
            additional_options = additional_options or {
                "observations.scope": "ALL",
                "performanceTuning.caching": "CACHE_NOTHING",
            }

            # Apply data quality rules
            dq_results = EvaluateDataQuality().process_rows(
                frame=dynamic_frame,
                ruleset=ruleset,
                publishing_options=publishing_options,
                additional_options=additional_options,
            )

            # Extract rule outcomes
            rule_outcomes = self._extract_rule_outcomes(dq_results, context_name)

            # Check for failed rules
            failed_rules_count = self._check_failed_rules(rule_outcomes)

            # Generate summary
            summary = {
                "context_name": context_name,
                "total_rules": rule_outcomes.count(),
                "failed_rules": failed_rules_count,
                "passed_rules": rule_outcomes.count() - failed_rules_count,
            }

            # Log summary
            if failed_rules_count > 0:
                logger.error(
                    f"Data quality check failed for {context_name}. "
                    f"{failed_rules_count} rules failed out of {summary['total_rules']}."
                )
            else:
                logger.info(f"All data quality rules passed for {context_name}.")

            logger.info(f"Data quality evaluation completed for {context_name}.")
            return summary

        except Exception as e:
            logger.exception(f"Error during data quality evaluation for {context_name}: {e}")
            raise

    def _extract_rule_outcomes(self, dq_results, context_name):
        """
        Extract rule outcomes from data quality results.

        :param dq_results: The DataFrameCollection from data quality evaluation.
        :param context_name: The context name for transformation.
        :return: A DynamicFrame containing rule outcomes.
        """
        logger.debug(f"Extracting rule outcomes for context: {context_name}...")
        rule_outcomes = SelectFromCollection.apply(
            dfc=dq_results, key="ruleOutcomes", transformation_ctx=f"{context_name}_ruleOutcomes"
        )
        logger.debug("Rule outcomes extracted successfully.")
        return rule_outcomes.toDF()

    def _check_failed_rules(self, rule_outcomes):
        """
        Check for failed rules in rule outcomes.

        :param rule_outcomes: The rule outcomes DataFrame.
        :return: The count of failed rules.
        """
        logger.debug("Checking for failed rules...")
        failed_count = rule_outcomes.filter("Outcome = 'Failed'").count()
        logger.debug(f"Failed rules count: {failed_count}")
        return failed_count
