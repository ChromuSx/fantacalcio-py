# convenienza_calculator.py - VERSIONE AGGIORNATA
import pandas as pd
import ast
from loguru import logger
from config import ANNO_CORRENTE

# --- Funzioni per FPEDIA con QUOTAZIONI ---

skills_mapping = {
    "Fuoriclasse": 1,
    "Titolare": 3,
    "Buona Media": 2,
    "Goleador": 4,
    "Assistman": 2,
    "Piazzati": 2,
    "Rigorista": 5,
    "Giovane talento": 2,
    "Panchinaro": -4,
    "Falloso": -2,
    "Outsider": 2,
}


def calcola_convenienza_fpedia(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcola tre indici di convenienza per i dati di FPEDIA:
    1. 'Convenienza': rapporto performance/quotazione (il più importante per l'asta)
    2. 'Convenienza Potenziale': basata su potenziale e skills
    3. 'Valore_su_Prezzo': indice diretto fantamedia/quotazione
    """
    if df.empty:
        logger.warning("DataFrame FPEDIA è vuoto. Calcolo saltato.")
        return df

    df_calc = df.copy()
    
    # Assicurati che le colonne numeriche siano nel formato corretto
    numeric_cols = [
        f"Fantamedia anno {ANNO_CORRENTE-2}-{ANNO_CORRENTE-1}",
        f"Fantamedia anno {ANNO_CORRENTE-1}-{ANNO_CORRENTE}",
        f"Presenze {ANNO_CORRENTE-1}-{ANNO_CORRENTE}",
        f"FM su tot gare {ANNO_CORRENTE-1}-{ANNO_CORRENTE}",
        "Presenze campionato corrente",
        "Punteggio",
        "Buon investimento",
        "Resistenza infortuni",
        "quotazione_attuale",  # NUOVO: quotazione reale
        "fantavoto_medio",      # NUOVO: FVM dal file quotazioni
    ]
    
    for col in numeric_cols:
        if col in df_calc.columns:
            df_calc[col] = pd.to_numeric(df_calc[col], errors="coerce").fillna(0)
    
    # Se non c'è la quotazione, usa un default basato sul punteggio
    if 'quotazione_attuale' not in df_calc.columns:
        logger.warning("Quotazioni non trovate, uso stima basata su Punteggio")
        df_calc['quotazione_attuale'] = (df_calc['Punteggio'] / 100 * 30).clip(lower=1)
    
    # --- 1. CONVENIENZA CLASSICA (Performance/Prezzo) ---
    res_convenienza = []
    giocatemax = df_calc["Presenze campionato corrente"].max()
    if giocatemax == 0:
        giocatemax = 1

    for _, row in df_calc.iterrows():
        quotazione = row.get('quotazione_attuale', 10)
        if quotazione == 0:
            quotazione = 1  # Evita divisione per zero
        
        # Calcola il valore del giocatore basato su performance
        valore_performance = 0
        
        # Fantamedia pesata per presenze
        fantamedia_corr = row.get(f"Fantamedia anno {ANNO_CORRENTE-1}-{ANNO_CORRENTE}", 0)
        fm_su_tot = row.get(f"FM su tot gare {ANNO_CORRENTE-1}-{ANNO_CORRENTE}", 0)
        presenze_corr = row.get(f"Presenze {ANNO_CORRENTE-1}-{ANNO_CORRENTE}", 0)
        
        fantamedia_effettiva = fm_su_tot if fm_su_tot > 0 else fantamedia_corr
        
        if presenze_corr > 5:
            valore_performance = fantamedia_effettiva * (presenze_corr / 38)
        
        # Aggiungi bonus da skills
        try:
            skills_list = ast.literal_eval(row.get("Skills", "[]"))
            skill_bonus = sum(skills_mapping.get(skill, 0) for skill in skills_list) * 0.5
            valore_performance += skill_bonus
        except:
            pass
        
        # Bonus/malus vari
        if row.get("Buon investimento", 0) > 60:
            valore_performance += 1
        if row.get("Resistenza infortuni", 0) > 60:
            valore_performance += 1
        if row.get("Infortunato", False):
            valore_performance -= 2
        if row.get("Trend", "") == "UP":
            valore_performance += 1
        
        # CONVENIENZA = Valore Performance / Quotazione * 100
        convenienza = (valore_performance / quotazione) * 100
        res_convenienza.append(convenienza)
    
    df["Convenienza"] = res_convenienza
    
    # --- 2. VALORE SU PREZZO (indice semplice ma efficace) ---
    # Questo è l'indice più diretto: fantamedia/prezzo
    df['Valore_su_Prezzo'] = 0
    mask = df_calc['quotazione_attuale'] > 0
    
    # Usa FVM se disponibile, altrimenti FM su tot gare
    if 'fantavoto_medio' in df_calc.columns:
        fm_da_usare = df_calc['fantavoto_medio'].where(
            df_calc['fantavoto_medio'] > 0, 
            df_calc[f"FM su tot gare {ANNO_CORRENTE-1}-{ANNO_CORRENTE}"]
        )
    else:
        fm_da_usare = df_calc[f"FM su tot gare {ANNO_CORRENTE-1}-{ANNO_CORRENTE}"]
    
    df.loc[mask, 'Valore_su_Prezzo'] = (fm_da_usare[mask] / df_calc.loc[mask, 'quotazione_attuale']) * 100
    
    # --- 3. CONVENIENZA POTENZIALE (per giocatori con poche presenze) ---
    res_potenziale = []
    for _, row in df_calc.iterrows():
        quotazione = row.get('quotazione_attuale', 10)
        if quotazione == 0:
            quotazione = 1
        
        # Base: punteggio FPEDIA + FVM dal file quotazioni
        potenziale = row.get("Punteggio", 50) / 10  # Normalizza a 0-10
        
        # Se abbiamo il FVM dalle quotazioni, usalo
        if 'fantavoto_medio' in row and row['fantavoto_medio'] > 0:
            potenziale += row['fantavoto_medio'] / 10
        
        # Bonus skills (più peso nel potenziale)
        try:
            skills_list = ast.literal_eval(row.get("Skills", "[]"))
            potenziale += sum(skills_mapping.get(skill, 0) for skill in skills_list)
        except:
            pass
        
        # Calcola convenienza potenziale
        convenienza_pot = (potenziale / quotazione) * 100
        res_potenziale.append(convenienza_pot)
    
    df["Convenienza Potenziale"] = res_potenziale
    
    logger.info("Indici di convenienza calcolati con quotazioni reali")
    
    # Log delle top 5 occasioni per Valore/Prezzo
    top_vsp = df.nlargest(5, 'Valore_su_Prezzo')[['Nome', 'Squadra', 'quotazione_attuale', 'Valore_su_Prezzo']]
    logger.info(f"Top 5 Valore/Prezzo:\n{top_vsp}")
    
    return df


def calcola_convenienza_FSTATS(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcola la convenienza per FSTATS usando le quotazioni reali.
    """
    if df.empty:
        logger.warning("DataFrame FSTATS è vuoto. Calcolo saltato.")
        return df

    df_calc = df.copy()
    
    # Colonne numeriche incluse le quotazioni
    numeric_cols = [
        "goals", "assists", "yellowCards", "redCards",
        "xgFromOpenPlays", "xA", "presences", 
        "fanta_avg", "fantacalcioFantaindex",
        "quotazione_attuale", "fantavoto_medio"
    ]
    
    for col in numeric_cols:
        if col in df_calc.columns:
            df_calc[col] = pd.to_numeric(df_calc[col], errors="coerce").fillna(0)
    
    # Default quotazione se mancante
    if 'quotazione_attuale' not in df_calc.columns:
        logger.warning("Quotazioni non trovate per FSTATS, uso default")
        df_calc['quotazione_attuale'] = 10
    
    # Assicurati che non ci siano quotazioni zero
    df_calc['quotazione_attuale'] = df_calc['quotazione_attuale'].replace(0, 1)
    
    # --- CONVENIENZA basata su performance/prezzo ---
    df_con_presenze = df_calc[df_calc["presences"] > 0].copy()
    
    if not df_con_presenze.empty:
        # Calcola valore totale del giocatore
        bonus_score = (df_con_presenze["goals"] * 3) + (df_con_presenze["assists"] * 1)
        malus_score = (df_con_presenze["yellowCards"] * 0.5) + (df_con_presenze["redCards"] * 1)
        
        # Valore per presenza
        valore_per_presenza = (
            df_con_presenze["fanta_avg"] + 
            (bonus_score / df_con_presenze["presences"]) -
            (malus_score / df_con_presenze["presences"])
        )
        
        # Convenienza = valore / quotazione
        df_con_presenze["Convenienza"] = (valore_per_presenza / df_con_presenze["quotazione_attuale"]) * 100
        
        # Merge back
        df = df.merge(df_con_presenze[["Nome", "Convenienza"]], on="Nome", how="left")
    else:
        df["Convenienza"] = 0
    
    # --- VALORE SU PREZZO semplice ---
    mask = df_calc['quotazione_attuale'] > 0
    df['Valore_su_Prezzo'] = 0
    
    # Usa fantavoto_medio se disponibile, altrimenti fanta_avg
    fm_da_usare = df_calc['fantavoto_medio'].where(
        df_calc['fantavoto_medio'] > 0,
        df_calc['fanta_avg']
    ) if 'fantavoto_medio' in df_calc.columns else df_calc['fanta_avg']
    
    df.loc[mask, 'Valore_su_Prezzo'] = (fm_da_usare[mask] / df_calc.loc[mask, 'quotazione_attuale']) * 100
    
    # --- CONVENIENZA POTENZIALE con xG e xA ---
    potential_value = (
        df_calc["fantacalcioFantaindex"] / 10 +  # Normalizza l'index
        (df_calc["xgFromOpenPlays"] + df_calc["xA"]) * 2  # Peso alto per expected stats
    )
    
    df["Convenienza Potenziale"] = (potential_value / df_calc["quotazione_attuale"]) * 100
    
    # Fill NaN
    df.fillna({"Convenienza": 0, "Convenienza Potenziale": 0, "Valore_su_Prezzo": 0}, inplace=True)
    
    logger.info("Convenienza FSTATS calcolata con quotazioni reali")
    
    return df