# data_unifier.py - Modulo dedicato per l'unificazione dei dataset
import pandas as pd
from loguru import logger
import config


def normalize_roles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizza i ruoli per avere consistenza tra FPEDIA e FSTATS.
    """
    role_mapping = {
        'POR': 'P',
        'DIF': 'D', 
        'CEN': 'C',
        'ATT': 'A',
        'Portiere': 'P',
        'Portieri': 'P',
        'Difensore': 'D',
        'Difensori': 'D',
        'Centrocampista': 'C',
        'Centrocampisti': 'C',
        'Attaccante': 'A',
        'Attaccanti': 'A'
    }
    
    if 'Ruolo' in df.columns:
        df['Ruolo'] = df['Ruolo'].replace(role_mapping)
        # Forza maiuscolo per sicurezza
        df['Ruolo'] = df['Ruolo'].str.upper()
        # Gestisci valori non mappati
        valid_roles = ['P', 'D', 'C', 'A']
        df.loc[~df['Ruolo'].isin(valid_roles), 'Ruolo'] = 'N/D'
    
    return df


def calculate_adjusted_index(row: pd.Series) -> float:
    """
    Calcola un indice aggiustato che bilancia meglio i ruoli.
    """
    base_index = row.get('Indice_Unificato', 0)
    if pd.isna(base_index) or base_index is None:
        base_index = 0
    
    ruolo = row.get('Ruolo', '')
    quotazione = row.get('quotazione_attuale', 10)
    if pd.isna(quotazione) or quotazione is None:
        quotazione = 10
    
    # FIX: Gestisci correttamente le presenze che potrebbero essere None
    presenze = row.get('Presenze campionato corrente', 0)
    if pd.isna(presenze) or presenze is None:
        presenze = 0
    
    # AGGIUSTAMENTO AGGRESSIVO PER RUOLO
    # I portieri hanno dinamiche diverse, quindi vanno valutati separatamente
    if ruolo == 'P':
        # Penalizzazione forte per i portieri nell'indice generale
        base_index = base_index * 0.4  # Riduzione del 60%
        
        # Bonus solo per portieri titolari certi
        if presenze > 15:
            base_index = base_index * 1.2
    
    # Bonus per giocatori di movimento con quotazioni medio-alte
    if ruolo in ['D', 'C', 'A']:
        if quotazione > 10 and quotazione <= 20:
            base_index = base_index * 1.1
        elif quotazione > 20 and quotazione <= 30:
            base_index = base_index * 1.15
        elif quotazione > 30:
            base_index = base_index * 1.2
        
        # Bonus per attaccanti e centrocampisti prolifici
        goals = row.get('goals', 0)
        if pd.isna(goals) or goals is None:
            goals = 0
        
        assists = row.get('assists', 0)
        if pd.isna(assists) or assists is None:
            assists = 0
        
        if ruolo == 'A' and goals > 10:
            base_index = base_index * 1.2
        elif ruolo == 'C' and (goals > 5 or assists > 5):
            base_index = base_index * 1.15
        elif ruolo == 'D' and goals > 2:
            base_index = base_index * 1.1
    
    # Bonus per giocatori con dati completi e affidabili
    if row.get('Fonte_Dati') == 'Entrambe':
        base_index = base_index * 1.1
    
    # Bonus per titolari certi (tante presenze)
    if presenze > 20:
        base_index = base_index * 1.05
    
    return base_index


def create_unified_dataset_improved(df_fpedia: pd.DataFrame, df_fstats: pd.DataFrame) -> pd.DataFrame:
    """
    Versione corretta dell'unificazione che elimina davvero i duplicati.
    Usa cognome + squadra per il matching e rimuove duplicati finali.
    """
    if df_fpedia.empty and df_fstats.empty:
        logger.warning("Entrambi i DataFrame sono vuoti")
        return pd.DataFrame()
    
    # Normalizza i ruoli in entrambi i dataset
    if not df_fstats.empty:
        df_fstats = normalize_roles(df_fstats.copy())
        logger.debug(f"FSTATS squadre esempio: {df_fstats['Squadra'].head(3).tolist()}")
    
    if not df_fpedia.empty:
        df_fpedia = normalize_roles(df_fpedia.copy())
        logger.debug(f"FPEDIA squadre esempio: {df_fpedia['Squadra'].head(3).tolist()}")
    
    # Se uno è vuoto, ritorna l'altro
    if df_fpedia.empty:
        return df_fstats
    if df_fstats.empty:
        return df_fpedia
    
    # APPROCCIO SEMPLIFICATO: Usa Nome e Squadra per il merge
    # Prima assicurati che le squadre siano comparabili
    df_fpedia['Squadra_norm'] = df_fpedia['Squadra'].str.lower().str.strip()
    df_fstats['Squadra_norm'] = df_fstats['Squadra'].str.lower().str.strip()
    
    # Crea chiave di merge basata su cognome + squadra
    # Per FPEDIA (formato: COGNOME NOME)
    df_fpedia['cognome'] = df_fpedia['Nome'].apply(
        lambda x: x.split()[0].lower() if pd.notna(x) and ' ' in x else str(x).lower()
    )
    
    # Per FSTATS (formato: Nome Cognome)
    df_fstats['cognome'] = df_fstats['Nome'].apply(
        lambda x: x.split()[-1].lower() if pd.notna(x) and ' ' in x else str(x).lower()
    )
    
    # Crea chiave di merge
    df_fpedia['merge_key'] = df_fpedia['cognome'] + '_' + df_fpedia['Squadra_norm']
    df_fstats['merge_key'] = df_fstats['cognome'] + '_' + df_fstats['Squadra_norm']
    
    # Log per debug
    logger.info(f"Chiavi merge FPEDIA esempio: {df_fpedia['merge_key'].head(5).tolist()}")
    logger.info(f"Chiavi merge FSTATS esempio: {df_fstats['merge_key'].head(5).tolist()}")
    
    # Trova le chiavi comuni
    common_keys = set(df_fpedia['merge_key']) & set(df_fstats['merge_key'])
    logger.info(f"Giocatori in comune trovati: {len(common_keys)}")
    
    # STEP 1: Crea dataset per giocatori presenti in entrambi
    df_both = pd.DataFrame()
    if len(common_keys) > 0:
        # Prendi i dati da FPEDIA per i giocatori in comune
        df_fpedia_common = df_fpedia[df_fpedia['merge_key'].isin(common_keys)].copy()
        # Aggiungi i dati FSTATS
        df_fstats_common = df_fstats[df_fstats['merge_key'].isin(common_keys)]
        
        # Seleziona solo le colonne FSTATS che non sono già in FPEDIA
        fstats_cols_to_merge = ['merge_key']
        possible_cols = ['fantacalcioFantaindex', 'fanta_avg', 'avg', 'presences', 
                        'goals', 'assists', 'xgFromOpenPlays', 'xA', 'yellowCards', 'redCards']
        for col in possible_cols:
            if col in df_fstats_common.columns:
                fstats_cols_to_merge.append(col)
        
        # Merge sui giocatori comuni
        df_both = pd.merge(
            df_fpedia_common,
            df_fstats_common[fstats_cols_to_merge],
            on='merge_key',
            how='inner'
        )
        df_both['Fonte_Dati'] = 'Entrambe'
    
    # STEP 2: Giocatori SOLO in FPEDIA (escludi quelli già in df_both)
    df_fpedia_only = df_fpedia[~df_fpedia['merge_key'].isin(common_keys)].copy()
    df_fpedia_only['Fonte_Dati'] = 'Solo FPEDIA'
    
    # STEP 3: Giocatori SOLO in FSTATS (escludi quelli già in df_both)
    df_fstats_only = df_fstats[~df_fstats['merge_key'].isin(common_keys)].copy()
    df_fstats_only['Fonte_Dati'] = 'Solo FSTATS'
    
    # Aggiungi colonne mancanti a df_fstats_only
    for col in df_fpedia.columns:
        if col not in df_fstats_only.columns and col not in ['merge_key', 'cognome', 'Squadra_norm']:
            df_fstats_only[col] = None
    
    # STEP 4: Concatena tutti i DataFrame
    frames_to_concat = []
    if not df_both.empty:
        frames_to_concat.append(df_both)
    if not df_fpedia_only.empty:
        frames_to_concat.append(df_fpedia_only)
    if not df_fstats_only.empty:
        frames_to_concat.append(df_fstats_only)
    
    df_unified = pd.concat(frames_to_concat, ignore_index=True)
    
    # Rimuovi colonne temporanee
    cols_to_drop = ['merge_key', 'cognome', 'Squadra_norm']
    df_unified = df_unified.drop(columns=[c for c in cols_to_drop if c in df_unified.columns])
    
    # Calcola gli indici
    # IMPORTANTE: Inizializza la colonna come float per evitare problemi di tipo
    df_unified['Indice_Unificato'] = 0.0
    
    # Per giocatori con entrambe le fonti
    mask_both = df_unified['Fonte_Dati'] == 'Entrambe'
    if mask_both.any() and 'fanta_avg' in df_unified.columns:
        valore_su_prezzo = df_unified.loc[mask_both, 'Valore_su_Prezzo'].fillna(0)
        fanta_avg = df_unified.loc[mask_both, 'fanta_avg'].fillna(0)
        quota = df_unified.loc[mask_both, 'quotazione_attuale'].fillna(1).replace(0, 1)
        
        df_unified.loc[mask_both, 'Indice_Unificato'] = (
            valore_su_prezzo * 0.6 + (fanta_avg / quota * 100) * 0.4
        )
    
    # Per giocatori solo FPEDIA
    mask_fpedia = df_unified['Fonte_Dati'] == 'Solo FPEDIA'
    if mask_fpedia.any():
        df_unified.loc[mask_fpedia, 'Indice_Unificato'] = df_unified.loc[mask_fpedia, 'Valore_su_Prezzo'].fillna(0)
    
    # Per giocatori solo FSTATS
    mask_fstats = df_unified['Fonte_Dati'] == 'Solo FSTATS'
    if mask_fstats.any() and 'fanta_avg' in df_unified.columns and 'quotazione_attuale' in df_unified.columns:
        fanta_avg_fstats = df_unified.loc[mask_fstats, 'fanta_avg'].fillna(0)
        quota_fstats = df_unified.loc[mask_fstats, 'quotazione_attuale'].fillna(1).replace(0, 1)
        
        df_unified.loc[mask_fstats, 'Indice_Unificato'] = fanta_avg_fstats / quota_fstats * 100
    
    # Calcola gli altri indici
    df_unified['Indice_Aggiustato'] = df_unified.apply(calculate_adjusted_index, axis=1)
    
    df_unified['Affidabilita_Dati'] = 50
    df_unified.loc[df_unified['Fonte_Dati'] == 'Entrambe', 'Affidabilita_Dati'] += 30
    df_unified.loc[df_unified['quotazione_attuale'] > 0, 'Affidabilita_Dati'] += 10
    
    # Aggiungi bonus per presenze se la colonna esiste
    if 'presences' in df_unified.columns:
        df_unified.loc[df_unified['presences'] > 10, 'Affidabilita_Dati'] += 10
    elif 'Presenze campionato corrente' in df_unified.columns:
        df_unified.loc[df_unified['Presenze campionato corrente'] > 10, 'Affidabilita_Dati'] += 10
    
    df_unified['Score_Affare'] = (
        df_unified['Indice_Aggiustato'] * 0.7 + 
        df_unified['Affidabilita_Dati'] * 0.3
    )
    
    # Ordina e pulisci
    df_unified = df_unified.sort_values(by='Score_Affare', ascending=False)
    
    # Rimuovi righe con dati insufficienti
    df_unified = df_unified[
        (df_unified['Nome'].notna()) & 
        (df_unified['Nome'] != '') &
        (df_unified['Ruolo'].notna()) &
        (df_unified['Ruolo'] != 'N/D')
    ]
    
    # IMPORTANTE: Rimuovi duplicati finali basati su Nome
    # Tieni solo la versione con Score_Affare più alto
    df_unified = df_unified.sort_values('Score_Affare', ascending=False)
    df_unified = df_unified.drop_duplicates(subset=['Nome'], keep='first')
    
    # Log statistiche finali
    logger.info(f"✅ Dataset unificato FINALE: {len(df_unified)} giocatori unici")
    logger.info(f"  - Con dati da entrambe: {(df_unified['Fonte_Dati'] == 'Entrambe').sum()}")
    logger.info(f"  - Solo FPEDIA: {(df_unified['Fonte_Dati'] == 'Solo FPEDIA').sum()}")
    logger.info(f"  - Solo FSTATS: {(df_unified['Fonte_Dati'] == 'Solo FSTATS').sum()}")
    
    return df_unified


def save_unified_excel_improved(df_unified: pd.DataFrame, output_path: str):
    """
    Salva il file Excel unificato con sheet ottimizzati e classifiche bilanciate.
    """
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        
        # Sheet 1: Dataset completo
        df_unified.to_excel(writer, sheet_name='Tutti', index=False)
        
        # Sheet 2-5: Top per ruolo (con indice aggiustato)
        for ruolo in ['P', 'D', 'C', 'A']:
            df_ruolo = df_unified[df_unified['Ruolo'] == ruolo].head(50)
            if not df_ruolo.empty:
                df_ruolo.to_excel(writer, sheet_name=f'{ruolo}_Top50', index=False)
        
        # Sheet 6: Super Affari BILANCIATI (escludi portieri dal top generale)
        # Crea due versioni: una senza portieri, una con max 2 portieri
        df_no_portieri = df_unified[df_unified['Ruolo'] != 'P'].nlargest(50, 'Score_Affare')
        top_portieri = df_unified[df_unified['Ruolo'] == 'P'].nlargest(2, 'Score_Affare')
        df_affari = pd.concat([df_no_portieri.head(48), top_portieri]).sort_values('Score_Affare', ascending=False)
        df_affari.to_excel(writer, sheet_name='Super_Affari', index=False)
        
        # Sheet 7: Migliori per ruolo (top 10 per ogni ruolo)
        migliori_ruolo = []
        for ruolo in ['P', 'D', 'C', 'A']:
            top_ruolo = df_unified[df_unified['Ruolo'] == ruolo].nlargest(10, 'Score_Affare')
            if not top_ruolo.empty:
                migliori_ruolo.append(top_ruolo)
        
        if migliori_ruolo:
            df_migliori = pd.concat(migliori_ruolo, ignore_index=True)
            # Ordina per ruolo e poi per score
            df_migliori = df_migliori.sort_values(['Ruolo', 'Score_Affare'], ascending=[True, False])
            df_migliori.to_excel(writer, sheet_name='Top10_Per_Ruolo', index=False)
        
        # Sheet 8: Occasioni Low Cost (quotazione <= 10, escludi portieri)
        df_lowcost = df_unified[
            (df_unified['quotazione_attuale'] <= 10) & 
            (df_unified['Ruolo'] != 'P') &
            (df_unified['Indice_Aggiustato'] > 30)
        ].nlargest(50, 'Score_Affare')
        if not df_lowcost.empty:
            df_lowcost.to_excel(writer, sheet_name='Occasioni_LowCost', index=False)
        
        # Sheet 9: Titolari affidabili (alta affidabilità + buone presenze)
        df_titolari = df_unified[
            (df_unified['Affidabilita_Dati'] >= 70) &
            (df_unified.get('Presenze campionato corrente', 0) >= 10)
        ].nlargest(50, 'Score_Affare')
        if not df_titolari.empty:
            df_titolari.to_excel(writer, sheet_name='Titolari_Affidabili', index=False)
        
        # Sheet 10: Giovani promesse (Nuovo acquisto o skills particolari)
        if 'Nuovo acquisto' in df_unified.columns:
            df_giovani = df_unified[
                (df_unified['Nuovo acquisto'] == True) |
                (df_unified['Skills'].str.contains('Giovane talento', na=False))
            ].nlargest(30, 'Score_Affare')
            if not df_giovani.empty:
                df_giovani.to_excel(writer, sheet_name='Nuovi_e_Giovani', index=False)
        
        # Sheet 11: Migliori Giocatori di Movimento (no portieri)
        df_movimento = df_unified[df_unified['Ruolo'].isin(['D', 'C', 'A'])]
        df_movimento = df_movimento.nlargest(60, 'Score_Affare')
        if not df_movimento.empty:
            df_movimento.to_excel(writer, sheet_name='Top_Movimento', index=False)
        
        # Sheet 12: Classifiche Separate per Fascia di Prezzo
        fasce_prezzo = [
            ('Low_1-10', 1, 10),
            ('Mid_11-20', 11, 20),
            ('High_21-30', 21, 30),
            ('Premium_30+', 31, 500)
        ]
        
        for nome_fascia, min_prezzo, max_prezzo in fasce_prezzo:
            df_fascia = df_unified[
                (df_unified['quotazione_attuale'] >= min_prezzo) &
                (df_unified['quotazione_attuale'] <= max_prezzo) &
                (df_unified['Ruolo'] != 'P')  # Escludi portieri dalle fasce di prezzo
            ].nlargest(20, 'Score_Affare')
            
            if not df_fascia.empty:
                df_fascia.to_excel(writer, sheet_name=f'Fascia_{nome_fascia}', index=False)
    
    logger.info(f"✅ File Excel migliorato salvato con classifiche bilanciate in: {output_path}")