def join_current_squad(token, league_id, today_df_results):
    squad_players = get_players_in_squad(token, league_id)

    # --- RESET-SAFE: empty squad ---
    if not squad_players or "it" not in squad_players or not squad_players["it"]:
        print("No squad players found. Skipping squad recommendations.")
        return pd.DataFrame(columns=[
            "last_name",
            "team_name",
            "mv",
            "mv_change_yesterday",
            "predicted_mv_target",
            "s_11_prob",
        ])

    squad_df = pd.DataFrame(squad_players["it"])

    # --- Robust detection of player-id column ---
    SQUAD_ID_COL = (
        "i" if "i" in squad_df.columns else
        "pi" if "pi" in squad_df.columns else
        None
    )

    if SQUAD_ID_COL is None:
        raise RuntimeError(
            f"Cannot determine squad player id column. Columns: {squad_df.columns.tolist()}"
        )

    # --- Merge with predictions ---
    squad_df = (
        pd.merge(
            today_df_results,
            squad_df,
            left_on="player_id",
            right_on=SQUAD_ID_COL,
        )
        .drop(columns=[SQUAD_ID_COL])
    )

    # Rename prob to s_11_prob
    if "prob" not in squad_df.columns:
        squad_df["prob"] = np.nan
    squad_df = squad_df.rename(columns={"prob": "s_11_prob"})

    # Rename mv_change_1d to mv_change_yesterday
    squad_df = squad_df.rename(columns={"mv_change_1d": "mv_change_yesterday"})

    # Rename mv_x to mv (merge artifact)
    if "mv_x" in squad_df.columns:
        squad_df = squad_df.rename(columns={"mv_x": "mv"})

    # Keep only relevant columns
    keep_cols = [
        "last_name",
        "team_name",
        "mv",
        "mv_change_yesterday",
        "predicted_mv_target",
        "s_11_prob",
    ]
    squad_df = squad_df[[c for c in keep_cols if c in squad_df.columns]]

    return squad_df




# TODO Add fail-safe check before player expires if the prob (starting 11) is still high, so no injuries or anything. if it dropped. dont bid / reccommend
def join_current_market(token, league_id, today_df_results):
    """Join the live predictions with the current market data to get bid recommendations"""

    players_on_market = get_league_players_on_market(token, league_id)

    # players_on_market to DataFrame
    market_df = pd.DataFrame(players_on_market)

    # Join market_df ("id") with today_df ("player_id")
    bid_df = (
        pd.merge(today_df_results, market_df, left_on="player_id", right_on="id")
        .drop(columns=["id"])
    )

    # exp contains seconds until expiration
    bid_df["hours_to_exp"] = np.round((bid_df["exp"] / 3600), 2)

    # check if current sysdate + hours_to_exp is after the next 22:00
    now = datetime.now(ZoneInfo("Europe/Berlin"))
    next_22 = now.replace(hour=22, minute=0, second=0, microsecond=0)
    diff = np.round((next_22 - now).total_seconds() / 3600, 2)

    # If hours_to_exp < diff then it expires today
    bid_df["expiring_today"] = bid_df["hours_to_exp"] < diff

    # Drop rows where predicted_mv_target is less than 5000
    bid_df = bid_df[bid_df["predicted_mv_target"] > 5000]

    # Sort by predicted_mv_target descending
    bid_df = bid_df.sort_values("predicted_mv_target", ascending=False)

    # Rename prob to s_11_prob for better understanding
    if "prob" not in bid_df.columns:
        bid_df["prob"] = np.nan  # Placeholder for non-pro users
    bid_df = bid_df.rename(columns={"prob": "s_11_prob"})

    # Rename mv_change_1d to mv_change_yesterday for better understanding
    bid_df = bid_df.rename(columns={"mv_change_1d": "mv_change_yesterday"})

    # Keep only relevant columns
    bid_df = bid_df[["last_name", "team_name", "mv", "mv_change_yesterday", "predicted_mv_target", "s_11_prob", "hours_to_exp", "expiring_today"]]

    return bid_df
