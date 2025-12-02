from pyspark.sql import SparkSession

from pyspark.dbutils import DBUtils
from royalties_extracts.common.common import verify_atlas_inmarket_royalty_counts, verify_non_zero_counts
from royalties_extracts.transformations.postmatch_live_US import process, writeDf

def run(start_day, end_day, env) -> None:
    spark = SparkSession.builder.appName("PostMatchLiveUSApp").getOrCreate()
    dbutils = DBUtils(spark)
    combined_df = process(spark, start_day, end_day,
                        f"""curated_{env}.royalties.item_royalties_daily""",
                        f"""foreign_redshift_edw_{env}.edw_datamart.dim_playlist""",
                        f"""foreign_redshift_edw_{env}.edw_datamart.dim_channel""",
                        f"""foreign_redshift_edw_{env}.edw_datamart.fact_performance_summary""",
                        f"""foreign_redshift_edw_{env}.edw_ods.v_consumption_type""",
                        f"""foreign_redshift_edw_{env}.edw_ods.v_streaming_device_group""",
                        f"""foreign_redshift_edw_{env}.edw_ods.v_consumption_source"""
                        )

    output_files_dict = writeDf(combined_df, env,end_day,dbutils)
    verify_non_zero_counts(spark, output_files_dict)

def main():
    import fire

    fire.Fire(run)

if __name__ == "__main__":
    main()
