# main.py
import os
from loguru import logger
import pandas as pd

import data_retriever
import data_processor
import convenienza_calculator
import config


def main():
    """
    Main script to run the entire Fantacalcio analysis pipeline.
    It now runs two separate pipelines for FPEDIA and FSTATS,
    generating both performance-based and potential-based convenience indexes.
    """
    os.makedirs(config.DATA_DIR, exist_ok=True)
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)

    logger.info("Starting Fantacalcio analysis pipeline...")

    # 1. Retrieve all data
    logger.info("Step 1: Retrieving data from all sources...")
    data_retriever.scrape_fpedia()
    data_retriever.fetch_FSTATS_data()
    logger.info("Data retrieval complete.")

    # 2. Load dataframes
    df_fpedia, df_FSTATS = data_processor.load_dataframes()

    # --- Pipeline for FPEDIA ---
    if not df_fpedia.empty:
        logger.info("--- Starting FPEDIA Pipeline ---")

        df_processed = data_processor.process_fpedia_data(df_fpedia)
        df_final = convenienza_calculator.calcola_convenienza_fpedia(df_processed)

        df_final = df_final.sort_values(by="Convenienza Potenziale", ascending=False)

        # Lista aggiornata con i campi effettivamente disponibili
        output_columns = [
            # Key Info
            "Nome",
            "Ruolo",
            "Squadra",
            # Calculated Indexes
            "Convenienza Potenziale",
            "Convenienza",
            "Punteggio",
            # Current Season Stats
            f"Fantamedia anno {config.ANNO_CORRENTE-1}-{config.ANNO_CORRENTE}",
            f"Presenze {config.ANNO_CORRENTE-1}-{config.ANNO_CORRENTE}",
            f"FM su tot gare {config.ANNO_CORRENTE-1}-{config.ANNO_CORRENTE}",
            f"Fanta Media {config.ANNO_CORRENTE-1}-{config.ANNO_CORRENTE}",
            "Presenze campionato corrente",
            # Previous Season Stats
            f"Fantamedia anno {config.ANNO_CORRENTE-2}-{config.ANNO_CORRENTE-1}",
            f"Fantamedia anno {config.ANNO_CORRENTE-3}-{config.ANNO_CORRENTE-2}",
            # Predicted Stats
            "Presenze previste",
            "Gol previsti",
            "Assist previsti",
            # Qualitative Info
            "Trend",
            "Skills",
            "Consigliato prossima giornata",
            "Buon investimento",
            "Resistenza infortuni",
            "Infortunato",
            "Nuovo acquisto",
        ]
        
        # Filtra solo le colonne che esistono effettivamente
        final_columns = [col for col in output_columns if col in df_final.columns]

        output_path = os.path.join(config.OUTPUT_DIR, "fpedia_analysis.xlsx")
        df_final[final_columns].to_excel(output_path, index=False)

        logger.info(f"FPEDIA analysis complete. Results saved to {output_path}")
    else:
        logger.warning("FPEDIA DataFrame is empty. Pipeline skipped.")

    # --- Pipeline for FSTATS ---
    if not df_FSTATS.empty:
        logger.info("--- Starting FSTATS Pipeline ---")

        df_processed = data_processor.process_FSTATS_data(df_FSTATS)
        df_final = convenienza_calculator.calcola_convenienza_FSTATS(df_processed)

        df_final = df_final.sort_values(by="Convenienza Potenziale", ascending=False)

        # Define a comprehensive and ordered list of columns for the final output
        output_columns = [
            # Key Info
            "Nome",
            "Ruolo",
            "Squadra",
            # Calculated Indexes
            "Convenienza Potenziale",
            "Convenienza",
            "fantacalcioFantaindex",
            # Key Performance Indicators
            "fanta_avg",
            "avg",
            "presences",
            # Core Stats
            "goals",
            "assists",
            # Potential Stats
            "xgFromOpenPlays",
            "xA",
            # Disciplinary
            "yellowCards",
            "redCards",
            # Other available fields (will be included if present)
            "injured",
            "banned",
            "mantra_position",
            "fantacalcio_position",
            "birth_date",
            "foot_name",
            "fantacalcioPlayerId",
            "fantacalcioTeamName",
            "appearances",
            "matchesInStart",
            "mins_played",
            "pagella",
            "fantacalcioRanking",
            "fantacalcioPosition",
            "goals90min",
            "goalsFromOpenPlays",
            "xgFromOpenPlays/90min",
            "xA90min",
            "successfulPenalties",
            "penalties",
            "gkPenaltiesSaved",
            "gkCleanSheets",
            "gkConcededGoals",
            "openPlaysGoalsConceded",
            "openPlaysXgConceded",
            "fantamediaPred",
            "fantamediaPredRoundId",
            "matchConvocation",
            "matchesWithGrade",
            "perc_matchesStarted",
            "perc_matchesWithGrade",
            "percMinsPlayed",
            "expectedFantamediaMean",
            "External_breakout_Index",
            "Shot_on_goal_Index",
            "Offensive_actions_Index",
            "Pass_forward_accuracy_Index",
            "Air_challenge_offensive_Index",
            "Cross_accuracy_Index",
            "Converge_in_the_center_Index",
            "Accompany_the_offensive_action_Index",
            "Offensive_verticalization_Index",
            "Received_pass_Index",
            "Attacking_area_Index",
            "Offensive_field_presence_Index",
            "Pass_accuracy_Index",
            "Pass_leading_chances_Index",
            "Deep_runs_Index",
            "Defense_solidity_Index",
            "Set_piece_attack_Index",
            "Shot_on_target_Index",
            "Dribbles_successful_Index",
        ]
        final_columns = [col for col in output_columns if col in df_final.columns]

        output_path = os.path.join(config.OUTPUT_DIR, "FSTATS_analysis.xlsx")
        df_final[final_columns].to_excel(output_path, index=False)

        logger.info(f"FSTATS analysis complete. Results saved to {output_path}")
    else:
        logger.warning("FSTATS DataFrame is empty. Pipeline skipped.")

    logger.info("Fantacalcio analysis pipeline finished.")


if __name__ == "__main__":
    main()