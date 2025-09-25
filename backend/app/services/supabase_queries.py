from typing import Optional, List, Dict, Any
from scripts.shared.supabase_utils import get_supabase

# ---- helpers ---------------------------------------------------------------

def _norm_row(r: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a row into a stable API shape:
      - player_id: string (do NOT cast to int; IDs are canonical and may have leading zeros)
      - player_name: optional string
      - team_id: int or None
      - team: optional string (fallback to str(team_id) if present)
    """
    pid = r.get("player_id")
    # ensure string
    pid = str(pid) if pid is not None else None

    # team_id may be int or str in DB; prefer int if possible
    tid_raw = r.get("team_id")
    try:
        tid = int(tid_raw) if tid_raw is not None else None
    except Exception:
        tid = None

    team_txt = r.get("team")
    if not team_txt and tid is not None:
        team_txt = str(tid)

    return {
        "player_id": pid,
        "player_name": r.get("player_name"),
        "team_id": tid,
        "team": team_txt,
    }

# ---- queries ---------------------------------------------------------------

def players_all() -> List[Dict[str, Any]]:
    sb = get_supabase()
    res = (
        sb.table("player_ids")
          .select("player_id, player_name, team_id, team")
          .order("team_id")
          .order("player_name")
          .execute()
    )
    data = res.data or []
    return [_norm_row(r) for r in data]

def player_lookup(player_id: Optional[str] = None,
                  player_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Prefer lookup by player_id; fallback to exact player_name if provided.
    """
    sb = get_supabase()
    if player_id:
        res = (
            sb.table("player_ids")
              .select("player_id, player_name, team_id, team")
              .eq("player_id", str(player_id))
              .limit(1).execute()
        )
        if res.data:
            return _norm_row(res.data[0])

    if player_name:
        res = (
            sb.table("player_ids")
              .select("player_id, player_name, team_id, team")
              .eq("player_name", player_name)
              .limit(1).execute()
        )
        if res.data:
            return _norm_row(res.data[0])

    return None

def players_search(q: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Search by player_name ILIKE OR player_id ILIKE.
    """
    sb = get_supabase()
    limit = max(1, min(limit, 50))
    # PostgREST OR filter: field.op.value,field.op.value
    or_clause = f"player_name.ilike.%{q}%,player_id.ilike.%{q}%"
    res = (
        sb.table("player_ids")
          .select("player_id, player_name, team_id, team")
          .or_(or_clause)
          .limit(limit)
          .execute()
    )
    data = res.data or []
    return [_norm_row(r) for r in data]

def players_by_team(team_id: Optional[int] = None,
                    team: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Prefer team_id; fallback to team (text). If both missing, return empty list.
    """
    sb = get_supabase()
    q = sb.table("player_ids").select("player_id, player_name, team_id, team")
    if team_id is not None:
        q = q.eq("team_id", team_id)
    elif team:
        q = q.eq("team", team)
    else:
        return []

    res = q.order("player_name").execute()
    data = res.data or []
    return [_norm_row(r) for r in data]
