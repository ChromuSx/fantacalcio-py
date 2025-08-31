# main.py - VERSIONE CON FILE UNIFICATO
import os
from loguru import logger
import pandas as pd

import data_retriever
import data_processor
import convenienza_calculator
import quotazioni_loader
import data_unifier  # NUOVO: modulo dedicato per unificazione
import config


def main():
    """
    Main script per l'analisi Fantacalcio con integrazione quotazioni e file unificato migliorato.
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
    
    # 3. Carica le quotazioni ufficiali
    logger.info("Step 2: Caricamento quotazioni ufficiali...")
    df_quotazioni = quotazioni_loader.load_quotazioni()
    
    if df_quotazioni.empty:
        logger.warning("⚠️ Quotazioni non disponibili - il calcolo userà stime basate su Punteggio")
    else:
        logger.info(f"✅ Caricate {len(df_quotazioni)} quotazioni ufficiali")

    # Variabili per conservare i dataset processati
    df_fpedia_final = pd.DataFrame()
    df_fstats_final = pd.DataFrame()

    # --- Pipeline FPEDIA con QUOTAZIONI ---
    if not df_fpedia.empty:
        logger.info("--- Starting FPEDIA Pipeline con Quotazioni ---")

        # Processa i dati FPEDIA
        df_processed = data_processor.process_fpedia_data(df_fpedia)
        
        # Merge con le quotazioni
        if not df_quotazioni.empty:
            df_processed = quotazioni_loader.merge_with_quotazioni(df_processed, df_quotazioni)
        
        # Calcola convenienza con quotazioni reali
        df_fpedia_final = convenienza_calculator.calcola_convenienza_fpedia(df_processed)

        # Ordina per il nuovo indice Valore_su_Prezzo
        df_fpedia_final = df_fpedia_final.sort_values(by="Valore_su_Prezzo", ascending=False)

        # Colonne output
        output_columns = [
            "Nome", "Ruolo", "Squadra",
            "quotazione_attuale", "Valore_su_Prezzo", "Convenienza", "Convenienza Potenziale", "fantavoto_medio",
            f"Fantamedia anno {config.ANNO_CORRENTE-1}-{config.ANNO_CORRENTE}",
            f"Presenze {config.ANNO_CORRENTE-1}-{config.ANNO_CORRENTE}",
            f"FM su tot gare {config.ANNO_CORRENTE-1}-{config.ANNO_CORRENTE}",
            "Presenze campionato corrente",
            f"Fantamedia anno {config.ANNO_CORRENTE-2}-{config.ANNO_CORRENTE-1}",
            "Presenze previste", "Gol previsti", "Assist previsti",
            "Punteggio", "Trend", "Skills",
            "Buon investimento", "Resistenza infortuni",
            "Infortunato", "Nuovo acquisto",
        ]
        
        final_columns = [col for col in output_columns if col in df_fpedia_final.columns]

        # Salva FPEDIA
        output_path = os.path.join(config.OUTPUT_DIR, "fpedia_analysis_con_quotazioni.xlsx")
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_fpedia_final[final_columns].to_excel(writer, sheet_name='Tutti', index=False)
            
            for ruolo in ['P', 'D', 'C', 'A']:
                df_ruolo = df_fpedia_final[df_fpedia_final['Ruolo'] == ruolo].head(30)
                if not df_ruolo.empty:
                    df_ruolo[final_columns].to_excel(writer, sheet_name=f'{ruolo}_Top30', index=False)
            
            df_occasioni = df_fpedia_final[df_fpedia_final['Valore_su_Prezzo'] > df_fpedia_final['Valore_su_Prezzo'].quantile(0.8)]
            df_occasioni[final_columns].to_excel(writer, sheet_name='Occasioni', index=False)

        logger.info(f"✅ FPEDIA analysis salvata in: {output_path}")

    # --- Pipeline FSTATS con QUOTAZIONI ---
    if not df_FSTATS.empty:
        logger.info("--- Starting FSTATS Pipeline con Quotazioni ---")

        # Processa i dati FSTATS
        df_processed = data_processor.process_FSTATS_data(df_FSTATS)
        
        # Merge con le quotazioni
        if not df_quotazioni.empty:
            df_processed = quotazioni_loader.merge_with_quotazioni(df_processed, df_quotazioni)
        
        # Calcola convenienza con quotazioni reali
        df_fstats_final = convenienza_calculator.calcola_convenienza_FSTATS(df_processed)

        # Ordina per Valore_su_Prezzo
        df_fstats_final = df_fstats_final.sort_values(by="Valore_su_Prezzo", ascending=False)

        # Colonne output
        output_columns = [
            "Nome", "Ruolo", "Squadra",
            "quotazione_attuale", "Valore_su_Prezzo", "Convenienza", "Convenienza Potenziale", "fantavoto_medio",
            "fantacalcioFantaindex", "fanta_avg", "avg", "presences",
            "goals", "assists", "xgFromOpenPlays", "xA",
            "yellowCards", "redCards",
        ]
        
        final_columns = [col for col in output_columns if col in df_fstats_final.columns]

        output_path = os.path.join(config.OUTPUT_DIR, "FSTATS_analysis_con_quotazioni.xlsx")
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_fstats_final[final_columns].to_excel(writer, sheet_name='Tutti', index=False)
            
            for ruolo in ['P', 'D', 'C', 'A']:
                df_ruolo = df_fstats_final[df_fstats_final['Ruolo'] == ruolo].head(30)
                if not df_ruolo.empty:
                    df_ruolo[final_columns].to_excel(writer, sheet_name=f'{ruolo}_Top30', index=False)

        logger.info(f"✅ FSTATS analysis salvata in: {output_path}")

    # --- NUOVO: Crea Dataset Unificato MIGLIORATO ---
    if not df_fpedia_final.empty or not df_fstats_final.empty:
        logger.info("--- Creazione Dataset Unificato MIGLIORATO ---")
        
        df_unified = data_unifier.create_unified_dataset_improved(df_fpedia_final, df_fstats_final)
        
        if not df_unified.empty:
            # Salva file unificato con il nuovo sistema migliorato
            output_path = os.path.join(config.OUTPUT_DIR, "analisi_unificata_fantacalcio.xlsx")
            data_unifier.save_unified_excel_improved(df_unified, output_path)
            
            # Log top 10 super affari
            logger.info("\n🏆 TOP 10 SUPER AFFARI (Score bilanciato):")
            top10 = df_unified.nlargest(10, 'Score_Affare')[
                ['Nome', 'Squadra', 'Ruolo', 'quotazione_attuale', 
                 'Indice_Aggiustato', 'Score_Affare', 'Affidabilita_Dati', 'Fonte_Dati']
            ]
            for _, row in top10.iterrows():
                logger.info(
                    f"  {row['Nome']} ({row['Squadra']}) - {row['Ruolo']} - "
                    f"Qt: {row['quotazione_attuale']:.0f} - Score: {row['Score_Affare']:.1f} - "
                    f"Affidab: {row['Affidabilita_Dati']:.0f}% - Fonte: {row['Fonte_Dati']}"
                )

    logger.info("\n✨ Pipeline completata con successo!")
    logger.info("📊 File generati in data/output/:")
    logger.info("  1. fpedia_analysis_con_quotazioni.xlsx")
    logger.info("  2. FSTATS_analysis_con_quotazioni.xlsx")
    logger.info("  3. analisi_unificata_fantacalcio.xlsx (BILANCIATO!)")
    logger.info("\n🎯 Usa il file unificato per trovare i migliori affari!")
    logger.info("   📌 Sheet principali:")
    logger.info("   - 'Super_Affari': le migliori occasioni (max 2 portieri)")
    logger.info("   - 'Top_Movimento': migliori D/C/A senza portieri")
    logger.info("   - 'Top10_Per_Ruolo': i migliori 10 per ogni ruolo")
    logger.info("   📊 Sheet per fascia di prezzo:")
    logger.info("   - 'Fascia_Low_1-10': occasioni economiche")
    logger.info("   - 'Fascia_Mid_11-20': giocatori fascia media")
    logger.info("   - 'Fascia_High_21-30': giocatori fascia alta")
    logger.info("   - 'Fascia_Premium_30+': top player")
    logger.info("   🎯 Sheet specializzati:")
    logger.info("   - 'Occasioni_LowCost': affari sotto i 10€")
    logger.info("   - 'Titolari_Affidabili': giocatori con molte presenze")
    logger.info("   - 'Nuovi_e_Giovani': nuovi acquisti e talenti")


if __name__ == "__main__":
    main()