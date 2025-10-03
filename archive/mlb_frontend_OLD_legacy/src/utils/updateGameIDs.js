// utils/updateGameIDs.js

import { supabase } from './supabaseClient.js'; // ✅ correct
import { getGamePkForTeamOnDate } from './fetchGameID.js';

async function updateMissingGameIDs() {
    const { data: props, error } = await supabase
        .from('player_props')
        .select('*')
        .is('game_id', null);

    if (error) {
        console.error('❌ Error fetching player props:', error);
        return;
    }

    for (const prop of props) {
        const { team, game_date, id } = prop;

        console.log(`🔍 Looking for game for team: ${team}, date: ${game_date}`);
        const game_id = await getGamePkForTeamOnDate(team, game_date);
        console.log('Fetched game ID:', game_id);

        if (game_id) {
            const { error: updateError } = await supabase
                .from('player_props')
                .update({ game_id })
                .eq('id', id);

            if (updateError) {
                console.error(`❌ Failed to update game_id for prop ID ${id}:`, updateError);
            } else {
                console.log(`✅ Updated prop ID ${id} with game_id ${game_id}`);
            }
        } else {
            console.warn(`⚠️ No game found for team ${team} on ${game_date}`);
        }
    }
}

updateMissingGameIDs();
