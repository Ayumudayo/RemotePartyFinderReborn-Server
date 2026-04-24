import fs from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { expect, test } from '@playwright/test';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '..');

function listingsShell() {
    return `<!doctype html>
<html class="no-js">
<head>
    <meta charset="utf-8">
    <script defer src="/assets/translations.js"></script>
    <script defer src="/assets/listing-data.js"></script>
    <script defer src="/assets/listings.js"></script>
</head>
<body>
    <div id="language" data-accept="en"></div>
    <div id="container">
        <div class="requires-js settings">
            <div class="controls">
                <input type="search" class="search" placeholder="Search">
                <select id="data-centre-filter">
                    <option value="All">All</option>
                    <option value="Mana">Mana</option>
                </select>
            </div>
            <div id="role-filter"></div>
            <div id="objective-filter"></div>
            <div id="condition-filter"></div>
            <input type="number" id="min-il-filter" min="0" max="999">
            <div id="content-type-filter"></div>
            <div class="content-select-container">
                <button type="button" id="content-select-trigger"><span>Select Content...</span></button>
                <div id="content-select-dropdown"><div id="content-select-list"></div></div>
            </div>
            <div id="selected-contents"></div>
        </div>
        <em id="listings-bootstrap-status" class="no-listings" data-i18n="loading_listings">Loading listings...</em>
        <em id="server-no-listings" class="no-listings" data-i18n="no_listings" style="display:none;">No listings</em>
        <em id="filter-no-listings" class="no-listings" data-i18n="no_filter_results" style="display:none;">No results</em>
        <div id="listings" class="list"></div>
        <nav class="pagination-controls requires-js">
            <a href="javascript:void(0)" class="page-btn prev">Previous</a>
            <ul class="pagination"></ul>
            <a href="javascript:void(0)" class="page-btn next">Next</a>
        </nav>
    </div>
    <button id="scroll-to-top"></button>
</body>
</html>`;
}

function leaderParse() {
    return {
        primary_percentile: null,
        primary_color_class: 'parse-none',
        secondary_percentile: null,
        secondary_color_class: 'parse-none',
        has_secondary: false,
        hidden: false,
        originally_hidden: false,
        estimated: false,
    };
}

function snapshot(revision, description) {
    return {
        revision,
        listings: [{
            time_left_seconds: 1800,
            updated_at_timestamp: 1_700_000_000 + revision,
            listing: {
                creator_name: [{ type: 'text', text: 'Smoke Tester' }],
                description: [{ type: 'text', text: description }],
                duty_id: 1010,
                duty_type: 1,
                category: 0,
                created_world: { name: 'Masamune' },
                home_world: { name: 'Masamune' },
                data_centre: 'Mana',
                min_item_level: 730,
                num_parties: 1,
                slot_count: 8,
                slots_filled_count: 1,
                high_end: true,
                cross_world: true,
                content_kind: 5,
                joinable_roles: 1,
                objective_bits: 1,
                conditions_bits: 2,
                search_area_bits: 1,
                description_badge_class: 'desc-blue',
                description_badges: ['duty_completion'],
                display_slots: [{
                    filled: true,
                    role_class: 'tank',
                    title: 'PLD',
                    icon_code: 'PLD',
                }],
                members: [{
                    name: 'Hidden Fallback',
                    home_world: { name: 'Masamune' },
                    job_id: 19,
                    job_code: 'PLD',
                    role_class: 'tank',
                    parse: {
                        primary_percentile: 84,
                        primary_color_class: 'parse-purple',
                        secondary_percentile: null,
                        secondary_color_class: 'parse-none',
                        has_secondary: false,
                        hidden: false,
                        originally_hidden: true,
                        estimated: false,
                    },
                    progress: {
                        final_boss_percentage: null,
                        final_clear_count: 5,
                    },
                    slot_index: 0,
                    party_index: 0,
                    fflogs_character_url: null,
                }],
                leader_parse: leaderParse(),
                is_alliance_view: false,
            },
        }],
    };
}

function contentType(filePath) {
    if (filePath.endsWith('.js')) return 'text/javascript; charset=utf-8';
    if (filePath.endsWith('.css')) return 'text/css; charset=utf-8';
    if (filePath.endsWith('.svg')) return 'image/svg+xml';
    return 'application/octet-stream';
}

async function serveAsset(requestPath, response) {
    const url = new URL(requestPath, 'http://localhost');
    const relative = url.pathname.replace(/^\/assets\//, '');
    const assetPath = path.join(root, 'assets', relative);
    const body = await fs.readFile(assetPath);
    response.writeHead(200, { 'Content-Type': contentType(assetPath) });
    response.end(body);
}

function startServer() {
    const server = http.createServer(async (request, response) => {
        try {
            if (request.url === '/listings') {
                response.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
                response.end(listingsShell());
                return;
            }

            if (request.url?.startsWith('/assets/')) {
                await serveAsset(request.url, response);
                return;
            }

            response.writeHead(404);
            response.end('not found');
        } catch (error) {
            response.writeHead(500);
            response.end(String(error));
        }
    });

    return new Promise((resolve) => {
        server.listen(0, '127.0.0.1', () => resolve(server));
    });
}

test('listings shell fetches once, then refreshes once for a revision notification', async ({ page }) => {
    const server = await startServer();
    const { port } = server.address();
    let snapshotRequests = 0;

    try {
        await page.addInitScript(() => {
            window.__rpfFakeSockets = [];
            window.__emitListingRevision = (revision) => {
                for (const socket of window.__rpfFakeSockets) {
                    socket.__dispatch('message', {
                        data: JSON.stringify({ type: 'listings_revision_changed', revision }),
                    });
                }
            };

            class FakeWebSocket {
                static CONNECTING = 0;
                static OPEN = 1;
                static CLOSING = 2;
                static CLOSED = 3;

                constructor(url) {
                    this.url = url;
                    this.readyState = FakeWebSocket.CONNECTING;
                    this.__listeners = new Map();
                    this.sent = [];
                    window.__rpfFakeSockets.push(this);
                    queueMicrotask(() => {
                        this.readyState = FakeWebSocket.OPEN;
                        this.__dispatch('open', {});
                    });
                }

                addEventListener(type, callback) {
                    const listeners = this.__listeners.get(type) || [];
                    listeners.push(callback);
                    this.__listeners.set(type, listeners);
                }

                send(data) {
                    this.sent.push(data);
                }

                close() {
                    this.readyState = FakeWebSocket.CLOSED;
                    this.__dispatch('close', {});
                }

                __dispatch(type, event) {
                    for (const callback of this.__listeners.get(type) || []) {
                        callback(event);
                    }
                }
            }

            window.WebSocket = FakeWebSocket;
        });

        await page.route('**/api/listings/snapshot', async (route) => {
            snapshotRequests += 1;
            await route.fulfill({
                status: 200,
                contentType: 'application/json; charset=utf-8',
                body: JSON.stringify(snapshot(
                    snapshotRequests,
                    snapshotRequests === 1 ? 'Initial snapshot' : 'Refreshed snapshot',
                )),
            });
        });

        await page.goto(`http://127.0.0.1:${port}/listings`, { waitUntil: 'domcontentloaded' });
        await expect(page.locator('.listing')).toHaveCount(1);
        await expect(page.locator('body')).toContainText('Initial snapshot');
        expect(snapshotRequests).toBe(1);

        await page.evaluate(() => window.__emitListingRevision(2));
        await expect(page.locator('body')).toContainText('Refreshed snapshot');
        expect(snapshotRequests).toBe(2);
        await expect(page.locator('.tag-hidden').first()).toHaveAttribute(
            'title',
            'FFLogs: Originally hidden player',
        );
    } finally {
        await new Promise((resolve) => server.close(resolve));
    }
});
