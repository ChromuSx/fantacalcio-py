# quotazioni_loader.py
import pandas as pd
import os
from loguru import logger
import config

def load_quotazioni() -> pd.DataFrame:
    """
    Carica il file delle quotazioni ufficiali del fantacalcio.
    
    Returns:
        DataFrame con le quotazioni o DataFrame vuoto se il file non esiste
    """
    quotazioni_file = os.path.join(config.DATA_DIR, "Quotazioni_Fantacalcio_Stagione_2025_26.xlsx")
    
    if not os.path.exists(quotazioni_file):
        logger.warning(f"File quotazioni non trovato: {quotazioni_file}")
        return pd.DataFrame()
    
    try:
        # Leggi il file Excel saltando la prima riga che contiene il titolo
        df = pd.read_excel(quotazioni_file, skiprows=1)
        
        # Rinomina le colonne per consistenza
        column_mapping = {
            'Id': 'id_giocatore',
            'R': 'ruolo_singolo',
            'RM': 'ruolo_mantra', 
            'Nome': 'nome',
            'Squadra': 'squadra',
            'Qt.A': 'quotazione_attuale',
            'Qt.I': 'quotazione_iniziale',
            'Diff.': 'diff_quotazione',
            'Qt.A M': 'quotazione_attuale_mantra',
            'Qt.I M': 'quotazione_iniziale_mantra',
            'Diff.M': 'diff_quotazione_mantra',
            'FVM': 'fantavoto_medio',
            'FVM M': 'fantavoto_medio_mantra'
        }
        df = df.rename(columns=column_mapping)
        
        # Normalizza i nomi per il matching (rimuovi spazi extra, converti in minuscolo)
        df['nome_normalizzato'] = df['nome'].str.strip().str.lower()
        
        # Converti quotazioni in numerico
        numeric_cols = ['quotazione_attuale', 'quotazione_iniziale', 
                       'fantavoto_medio', 'fantavoto_medio_mantra']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        logger.info(f"Caricate {len(df)} quotazioni dal file Excel")
        logger.debug(f"Range quotazioni: {df['quotazione_attuale'].min()}-{df['quotazione_attuale'].max()}")
        
        return df
        
    except Exception as e:
        logger.error(f"Errore nel caricamento delle quotazioni: {e}")
        return pd.DataFrame()


def merge_with_quotazioni(df_players: pd.DataFrame, df_quotazioni: pd.DataFrame) -> pd.DataFrame:
    """
    Unisce i dati dei giocatori con le quotazioni ufficiali.
    
    Args:
        df_players: DataFrame con i dati dei giocatori (FPEDIA o FSTATS)
        df_quotazioni: DataFrame con le quotazioni ufficiali
        
    Returns:
        DataFrame unito con le quotazioni
    """
    if df_quotazioni.empty:
        logger.warning("DataFrame quotazioni vuoto, skip del merge")
        df_players['quotazione_attuale'] = 10  # Default fallback
        df_players['fantavoto_medio'] = 0
        return df_players
    
    # Normalizza i nomi nel dataframe dei giocatori
    if 'Nome' in df_players.columns:
        df_players['nome_normalizzato'] = df_players['Nome'].str.strip().str.lower()
    elif 'nome' in df_players.columns:
        df_players['nome_normalizzato'] = df_players['nome'].str.strip().str.lower()
    else:
        logger.error("Colonna Nome non trovata nel DataFrame")
        return df_players
    
    # Prepara un subset delle quotazioni con solo le colonne necessarie
    quotazioni_subset = df_quotazioni[['nome_normalizzato', 'quotazione_attuale', 
                                       'quotazione_iniziale', 'fantavoto_medio']].copy()
    
    # Merge sui nomi normalizzati
    df_merged = df_players.merge(
        quotazioni_subset, 
        on='nome_normalizzato', 
        how='left',
        suffixes=('', '_quot')
    )
    
    # Rimuovi la colonna temporanea
    df_merged = df_merged.drop(columns=['nome_normalizzato'])
    
    # Gestisci i valori mancanti (giocatori non trovati nelle quotazioni)
    if 'quotazione_attuale' in df_merged.columns:
        # Stima una quotazione di default basata sul ruolo se mancante
        ruolo_defaults = {'P': 5, 'D': 8, 'C': 10, 'A': 12}
        
        for ruolo, default_val in ruolo_defaults.items():
            mask = (df_merged['quotazione_attuale'].isna()) & (df_merged['Ruolo'] == ruolo)
            df_merged.loc[mask, 'quotazione_attuale'] = default_val
        
        # Default generico per ruoli non mappati
        df_merged['quotazione_attuale'] = df_merged['quotazione_attuale'].fillna(10)
        df_merged['fantavoto_medio'] = df_merged['fantavoto_medio'].fillna(0)
    
    matched = df_merged['quotazione_attuale'].notna().sum()
    total = len(df_merged)
    logger.info(f"Match quotazioni: {matched}/{total} giocatori ({matched/total*100:.1f}%)")
    
    return df_merged