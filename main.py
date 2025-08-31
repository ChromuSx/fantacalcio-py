# main.py - VERSIONE AGGIORNATA
import os
from loguru import logger
import pandas as pd

import data_retriever
import data_processor
import convenienza_calculator
import quotazioni_loader  # NUOVO IMPORT
import config


def main():
    """
    Main script per l'analisi Fantacalcio con integrazione quotazioni ufficiali.
    """
    os.makedirs(config.DATA_DIR, exist_ok=True)
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)

    logger.info("Starting Fantacalcio analysis pipeline con quotazioni...")

    # 1. Recupera tutti i dati
    logger.info("Step 1: Retrieving data from all sources...")
    data_retriever.scrape_fpedia()
    data_retriever.fetch_FSTATS_data()
    logger.info("Data retrieval complete.")

    # 2. Carica dataframes
    df_fpedia, df_FSTATS = data_processor.load_dataframes()
    
    # 3. NUOVO: Carica le quotazioni ufficiali
    logger.info("Step 2: Caricamento quotazioni ufficiali...")
    df_quotazioni = quotazioni_loader.load_quotazioni()
    
    if df_quotazioni.empty:
        logger.warning("⚠️ Quotazioni non disponibili - il calcolo userà stime basate su Punteggio")
    else:
        logger.info(f"✅ Caricate {len(df_quotazioni)} quotazioni ufficiali")

    # --- Pipeline FPEDIA con QUOTAZIONI ---
    if not df_fpedia.empty:
        logger.info("--- Starting FPEDIA Pipeline con Quotazioni ---")

        # Processa i dati FPEDIA
        df_processed = data_processor.process_fpedia_data(df_fpedia)
        
        # NUOVO: Merge con le quotazioni
        if not df_quotazioni.empty:
            df_processed = quotazioni_loader.merge_with_quotazioni(df_processed, df_quotazioni)
        
        # Calcola convenienza con quotazioni reali
        df_final = convenienza_calculator.calcola_convenienza_fpedia(df_processed)

        # Ordina per il nuovo indice Valore_su_Prezzo (più affidabile)
        df_final = df_final.sort_values(by="Valore_su_Prezzo", ascending=False)

        # Colonne output aggiornate
        output_columns = [
            # Info chiave
            "Nome",
            "Ruolo", 
            "Squadra",
            
            # NUOVI INDICI CON QUOTAZIONI
            "quotazione_attuale",      # Prezzo reale all'asta
            "Valore_su_Prezzo",        # Indice principale: FM/Prezzo
            "Convenienza",             # Performance totale/Prezzo
            "Convenienza Potenziale",  # Per giocatori con poche presenze
            "fantavoto_medio",         # FVM dal file quotazioni
            
            # Stats stagione corrente
            f"Fantamedia anno {config.ANNO_CORRENTE-1}-{config.ANNO_CORRENTE}",
            f"Presenze {config.ANNO_CORRENTE-1}-{config.ANNO_CORRENTE}",
            f"FM su tot gare {config.ANNO_CORRENTE-1}-{config.ANNO_CORRENTE}",
            "Presenze campionato corrente",
            
            # Stats stagione precedente
            f"Fantamedia anno {config.ANNO_CORRENTE-2}-{config.ANNO_CORRENTE-1}",
            
            # Stats previste
            "Presenze previste",
            "Gol previsti",
            "Assist previsti",
            
            # Info qualitative
            "Punteggio",
            "Trend",
            "Skills",
            "Buon investimento",
            "Resistenza infortuni",
            "Infortunato",
            "Nuovo acquisto",
        ]
        
        # Filtra solo le colonne esistenti
        final_columns = [col for col in output_columns if col in df_final.columns]

        # Salva con nome diverso per distinguere la versione con quotazioni
        output_path = os.path.join(config.OUTPUT_DIR, "fpedia_analysis_con_quotazioni.xlsx")
        
        # Crea anche sheet separati per ruolo
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Sheet completo
            df_final[final_columns].to_excel(writer, sheet_name='Tutti', index=False)
            
            # Sheet per ruolo (top 30 per ogni ruolo)
            for ruolo in ['P', 'D', 'C', 'A']:
                df_ruolo = df_final[df_final['Ruolo'] == ruolo].head(30)
                if not df_ruolo.empty:
                    df_ruolo[final_columns].to_excel(writer, sheet_name=f'{ruolo}_Top30', index=False)
            
            # Sheet "Occasioni" - giocatori con ottimo Valore/Prezzo
            df_occasioni = df_final[df_final['Valore_su_Prezzo'] > df_final['Valore_su_Prezzo'].quantile(0.8)]
            df_occasioni[final_columns].to_excel(writer, sheet_name='Occasioni', index=False)

        logger.info(f"✅ FPEDIA analysis con quotazioni salvata in: {output_path}")
        
        # Log top 10 occasioni
        logger.info("\n🎯 TOP 10 OCCASIONI (Valore/Prezzo):")
        top10 = df_final.nlargest(10, 'Valore_su_Prezzo')[['Nome', 'Squadra', 'Ruolo', 'quotazione_attuale', 'Valore_su_Prezzo']]
        for _, row in top10.iterrows():
            logger.info(f"  {row['Nome']} ({row['Squadra']}) - {row['Ruolo']} - Qt: {row['quotazione_attuale']} - V/P: {row['Valore_su_Prezzo']:.1f}")

    # --- Pipeline FSTATS con QUOTAZIONI ---
    if not df_FSTATS.empty:
        logger.info("--- Starting FSTATS Pipeline con Quotazioni ---")

        # Processa i dati FSTATS
        df_processed = data_processor.process_FSTATS_data(df_FSTATS)
        
        # NUOVO: Merge con le quotazioni
        if not df_quotazioni.empty:
            df_processed = quotazioni_loader.merge_with_quotazioni(df_processed, df_quotazioni)
        
        # Calcola convenienza con quotazioni reali
        df_final = convenienza_calculator.calcola_convenienza_FSTATS(df_processed)

        # Ordina per Valore_su_Prezzo
        df_final = df_final.sort_values(by="Valore_su_Prezzo", ascending=False)

        # Colonne output
        output_columns = [
            # Info chiave
            "Nome",
            "Ruolo",
            "Squadra",
            
            # NUOVI INDICI
            "quotazione_attuale",
            "Valore_su_Prezzo",
            "Convenienza",
            "Convenienza Potenziale",
            "fantavoto_medio",
            
            # KPI
            "fantacalcioFantaindex",
            "fanta_avg",
            "avg",
            "presences",
            
            # Stats
            "goals",
            "assists",
            "xgFromOpenPlays",
            "xA",
            "yellowCards",
            "redCards",
        ]
        
        final_columns = [col for col in output_columns if col in df_final.columns]

        output_path = os.path.join(config.OUTPUT_DIR, "FSTATS_analysis_con_quotazioni.xlsx")
        
        # Salva con sheet multipli
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_final[final_columns].to_excel(writer, sheet_name='Tutti', index=False)
            
            # Sheet per ruolo
            for ruolo in ['P', 'D', 'C', 'A']:
                df_ruolo = df_final[df_final['Ruolo'] == ruolo].head(30)
                if not df_ruolo.empty:
                    df_ruolo[final_columns].to_excel(writer, sheet_name=f'{ruolo}_Top30', index=False)

        logger.info(f"✅ FSTATS analysis con quotazioni salvata in: {output_path}")

    logger.info("\n✨ Pipeline completata con successo! Controlla i file Excel in data/output/")


if __name__ == "__main__":
    main()