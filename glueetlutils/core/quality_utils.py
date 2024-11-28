from awsgluedq.transforms import EvaluateDataQuality
from awsglue.transforms import SelectFromCollection

class QualityUtils:
    
    def evaluate_data_quality(self, dynamic_frame, ruleset, context_name, logger):
        try:
            logger.info(f"Evaluating data quality for context: {context_name}...")
            
            # Apply data quality rules
            dq_results = EvaluateDataQuality().process_rows(
                frame=dynamic_frame,
                ruleset=ruleset,
                publishing_options={
                    "dataQualityEvaluationContext": context_name,
                    "enableDataQualityCloudWatchMetrics": True,
                    "enableDataQualityResultsPublishing": True
                },
                additional_options={
                    "observations.scope": "ALL",
                    "performanceTuning.caching": "CACHE_NOTHING"
                }
            )

            # Get rule outcomes
            rule_outcomes = SelectFromCollection.apply(
                dfc=dq_results,
                key="ruleOutcomes",
                transformation_ctx=f"{context_name}_ruleOutcomes"
            )

            # Log the results
            logger.info(f"Data Quality Results for {context_name}:")
            rule_outcomes.toDF().show(truncate=False)

            # Check for failures
            failed_rules = rule_outcomes.toDF().filter("Outcome = 'Failed'").count()
            if failed_rules > 0:
                logger.error(f"Data quality check failed for {context_name}. {failed_rules} rules failed.")

            logger.info(f"Data quality evaluation completed for {context_name}")

        except Exception as e:
            logger.error(f"Error during data quality evaluation for {context_name}: {e}")
            raise