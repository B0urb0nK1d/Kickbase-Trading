from kickbase_api.user import get_budget, get_username
from kickbase_api.league import (
    get_league_activities,
    get_league_ranking,
)
from kickbase_api.manager import (
    get_managers,
    get_manager_performance,
    get_manager_info,
)
from kickbase_api.others import get_achievement_reward
import pandas as pd


def calc_manager_budgets(token, league_id, league_start_date, start_budget):
    """Calculate manager budgets based on activities, bonuses, and team performance."""

    # --- Fetch activities ---
    try:
        activities, login_bonus, achievement_bonus = get_league_activities(
            token, league_id, league_start_date
        )
    except Exception as e:
        raise RuntimeError(f"Failed to fetch activities: {e}")

    activities_df = pd.DataFrame(activities)

    # --- DEBUG (temporär, zum Verifizieren der API) ---
    print("ACTIVITY COLUMNS:", activities_df.columns.tolist())
    print(activities_df.head())
    # -------------------------------------------------

    # --- Robust column detection ---
    buyer_col = "byr" if "byr" in activities_df.columns else (
        "usr" if "usr" in activities_df.columns else None
    )
    seller_col = "slr" if "slr" in activities_df.columns else None
    price_col = "trp" if "trp" in activities_df.columns else None

    if buyer_col is None or price_col is None:
        raise RuntimeError(
            f"Cannot determine buyer/price columns. Columns: {activities_df.columns.tolist()}"
        )

    # --- Bonuses ---
    total_login_bonus = sum(
        entry.get("data", {}).get("bn", 0) for entry in login_bonus
    )

    total_achievement_bonus = 0
    for item in achievement_bonus:
        try:
            a_id = item.get("data", {}).get("t")
            if a_id is None:
                continue
            amount, reward = get_achievement_reward(token, league_id, a_id)
            total_achievement_bonus += amount * reward
        except Exception as e:
            print(f"Warning: Failed to process achievement bonus {item}: {e}")

    # --- Manager performances ---
    managers = get_managers(token, league_id)
    performances = []

    for manager_name, manager_id in managers:
        try:
            info = get_manager_info(token, league_id, manager_id)
            team_value = info.get("tv", 0)

            perf = get_manager_performance(token, league_id, manager_id, manager_name)
            perf["Team Value"] = team_value
            performances.append(perf)
        except Exception as e:
            print(f"Warning: Skipping manager {manager_name}: {e}")

    perf_df = pd.DataFrame(performances)
    if not perf_df.empty:
        perf_df["point_bonus"] = perf_df["tp"].fillna(0) * 1000
    else:
        perf_df = pd.DataFrame(columns=["name", "point_bonus", "Team Value"])

    # --- Initial budgets ---
    users = set(activities_df[buyer_col].dropna().unique())
    if seller_col:
        users |= set(activities_df[seller_col].dropna().unique())

    budgets = {user: start_budget for user in users}

    for _, row in activities_df.iterrows():
        buyer = row.get(buyer_col)
        seller = row.get(seller_col) if seller_col else None
        price = row.get(price_col, 0)

        if pd.notna(buyer):
            budgets[buyer] -= price
        if pd.notna(seller):
            budgets[seller] += price

    budget_df = pd.DataFrame(list(budgets.items()), columns=["User", "Budget"])

    # --- Merge performance bonuses ---
    budget_df = budget_df.merge(
        perf_df[["name", "point_bonus", "Team Value"]],
        left_on="User",
        right_on="name",
        how="left",
    ).drop(columns=["name"], errors="ignore")

    budget_df["Budget"] += budget_df["point_bonus"].fillna(0)
    budget_df.drop(columns=["point_bonus"], inplace=True, errors="ignore")

    # --- Login bonus ---
    budget_df["Budget"] += total_login_bonus

    # --- Achievement bonus (estimation) ---
    for user in budget_df["User"]:
        bonus = calc_achievement_bonus_by_points(
            token, league_id, user, total_achievement_bonus
        )
        budget_df.loc[budget_df["User"] == user, "Budget"] += bonus

    # --- Sync own budget ---
    try:
        own_budget = get_budget(token, league_id)
        own_username = get_username(token)
        budget_df.loc[budget_df["User"] == own_username, "Budget"] = own_budget
    except Exception as e:
        print(f"Warning: Could not sync own budget: {e}")

    # --- Available budget calculation ---
    budget_df["Max Negative"] = (
        budget_df["Team Value"].fillna(0) + budget_df["Budget"]
    ) * -0.33

    budget_df["Available Budget"] = (
        budget_df["Max Negative"].fillna(0) - budget_df["Budget"]
    ) * -1

    budget_df.sort_values(
        "Available Budget", ascending=False, inplace=True, ignore_index=True
    )

    return budget_df


def calc_achievement_bonus_by_points(token, league_id, username, anchor_achievement_bonus):
    """Estimate achievement bonus for a user based on their total points compared to anchor user."""

    ranking = get_league_ranking(token, league_id)
    ranking_df = pd.DataFrame(ranking, columns=["Name", "Total Points"])

    if ranking_df.empty:
        return 0

    anchor_user = get_username(token)
    anchor_row = ranking_df[ranking_df["Name"] == anchor_user]
    if anchor_row.empty:
        return 0

    anchor_points = anchor_row["Total Points"].values[0]

    if username == anchor_user:
        return anchor_achievement_bonus

    user_row = ranking_df[ranking_df["Name"] == username]
    if user_row.empty:
        return 0

    user_points = user_row["Total Points"].values[0]
    scale = 1.0 if anchor_points == 0 else user_points / anchor_points

    return anchor_achievement_bonus * scale


def calc_achievement_bonus_by_rank(token, league_id, username, anchor_achievement_bonus):
    """Estimate achievement bonus for a user based on their ranking (currently unused)."""

    ranking = get_league_ranking(token, league_id)
    ranking_df = pd.DataFrame(ranking, columns=["Name", "Total Points"])

    if ranking_df.empty:
        return 0

    anchor_user = get_username(token)
    anchor_row = ranking_df[ranking_df["Name"] == anchor_user]
    if anchor_row.empty:
        return 0

    anchor_rank = anchor_row.index[0] + 1

    if username == anchor_user:
        return anchor_achievement_bonus

    user_row = ranking_df[ranking_df["Name"] == username]
    if user_row.empty:
        return 0

    user_rank = user_row.index[0] + 1
    rank_diff = anchor_rank - user_rank
    scale = 1.0 + (rank_diff * 0.1)

    return anchor_achievement_bonus * scale
