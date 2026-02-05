
import { MetabaseClient } from './src/metabase/client.js';
import dotenv from 'dotenv';
import { join } from 'path';

dotenv.config({ path: join(process.cwd(), '.env') });

async function main() {
    console.log("🚀 Probing Data Visibility...");

    // Remove API KEY from options to FORCE Username/Password auth
    const client = new MetabaseClient({
        url: process.env.METABASE_URL,
        username: process.env.METABASE_USERNAME,
        password: process.env.METABASE_PASSWORD
    });

    try {
        await client.authenticate();
        console.log("✅ API Authenticated (User/Pass)");
    } catch (e) {
        console.error("❌ Authentication Failed:", e.message);
        process.exit(1);
    }

    const internalDbId = 6;
    try {
        console.log(`\n🔍 Querying report_card on DB ${internalDbId}...`);
        const res = await client.executeNativeQuery(internalDbId, "SELECT count(*) FROM report_card", { enforcePrefix: false });

        if (res.rows && res.rows.length > 0) {
            console.log(`✅ SUCCESS: Found ${res.rows[0][0]} cards in report_card!`);
        } else {
            console.log("⚠️ Query success but returned 0 rows.");
        }
    } catch (e) {
        console.error(`❌ Failed to query Internal DB (ID ${internalDbId}):`, e.message);
    }
}

main().catch(err => console.error(err));
