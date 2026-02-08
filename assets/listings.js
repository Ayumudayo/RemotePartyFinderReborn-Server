(function () {
    let stateWasNull = false;

    const state = {
        allowed: [],
        centre: 'All',
        roles: 0n,
        list: null,
        lang: null,
        highEnd: true, // Default to true
        objectives: 0,
        conditions: 0,
        onePlayerPerJob: false,
        minItemLevel: 0,
        contentTypes: [], // ContentKind 값 배열 (28=Ultimate, 5=Savage, 4=Extreme, 37=Chaotic)
        selectedContents: [], // 선택된 콘텐츠 이름 배열
    };

    function addJsClass() {
        document.children[0].className = 'js';
    }

    function saveLoadState() {
        let saved = localStorage.getItem('state');
        if (saved !== null) {
            try {
                saved = JSON.parse(saved, (key, value) => key === 'roles' ?
                    BigInt(value) : value);
                if (!Array.isArray(saved.allowed)) {
                    saved = {};
                    stateWasNull = true;
                }
            } catch (e) {
                saved = {};
                stateWasNull = true;
            }

            for (let key in saved) {
                if (state.hasOwnProperty(key)) {
                    state[key] = saved[key];
                }
            }
        } else {
            stateWasNull = true;
        }

        window.addEventListener('pagehide', () => {
            let copy = {};
            for (let key in state) {
                if (key === 'list') {
                    continue;
                }

                copy[key] = state[key];
            }

            localStorage.setItem('state', JSON.stringify(copy, (_, value) =>
                typeof value === 'bigint' ? value.toString() : value));
        });

        // Min Item Level
        const minILFilter = document.getElementById('min-il-filter');
        if (minILFilter) {
            minILFilter.addEventListener('input', (e) => {
                let val = parseInt(e.target.value);
                if (isNaN(val) || val < 0) val = 0;
                state.minItemLevel = val;
                refilter();
            });
        }
    }

    function reflectState() {
        // category-filter was removed from the template
        // let category = document.getElementById('category-filter');
        // for (let option of category.options) {
        //     if (stateWasNull) {
        //         console.log('was null');
        //         state.allowed.push(option.value);
        //     }
        //     option.selected = state.allowed.includes(option.value);
        // }

        let dataCentre = document.getElementById('data-centre-filter');
        dataCentre.value = state.centre;

        if (stateWasNull || state.roles <= 0n) {
            state.roles = 0n;
        } else {
            let roleFilterInputs = document.getElementById('role-filter')
                .getElementsByTagName('input');
            let newRolesState = 0n;
            for (let input of roleFilterInputs) {
                let value = BigInt(input.value);
                if (state.roles & value) {
                    input.checked = true;
                    newRolesState |= value;
                }
            }
            state.roles = newRolesState;
        }

        // High-end 필터는 항상 활성화 (버튼 제거됨)
        state.highEnd = true;

        const objectiveInputs = document.getElementById('objective-filter').getElementsByTagName('input');
        for (let input of objectiveInputs) {
            let val = parseInt(input.value);
            input.checked = (state.objectives & val) !== 0;
        }

        const conditionInputs = document.getElementById('condition-filter').getElementsByTagName('input');
        for (let input of conditionInputs) {
            let val = parseInt(input.value);
            if (val === 32) { // One Player per Job
                input.checked = state.onePlayerPerJob;
            } else {
                input.checked = (state.conditions & val) !== 0;
            }
        }



        const minILInput = document.getElementById('min-il-filter');
        if (minILInput) {
            minILInput.value = state.minItemLevel || 0;
        }

        // Language priority: cookie > dataset.accept > localStorage
        let language = document.getElementById('language');
        let cookieMatch = document.cookie.match(/(?:^|; )lang=([^;]*)/);

        if (cookieMatch) {
            state.lang = decodeURIComponent(cookieMatch[1]);
        } else if (language && language.dataset.accept) {
            state.lang = language.dataset.accept;
        } else if (state.lang === null) {
            state.lang = 'en'; // fallback
        }
    }

    function setUpList() {
        const listEl = document.getElementById('listings');
        if (!listEl || listEl.children.length === 0) {
            // 리스트가 비어있으면 초기화 에러를 방지하기 위해 Mock 객체 반환
            return {
                filter: function () { },
                on: function () { },
                items: [],
                visibleItems: [],
                size: function () { return 0; },
                show: function () { },
                reIndex: function () { },
                i: 1,
                page: 50,
                matchingItems: []
            };
        }

        let options = {
            valueNames: [
                'duty',
                'creator',
                'description',
                { data: ['centre'] },
            ],
            page: 50,
            pagination: {
                innerWindow: 2,
                outerWindow: 1,
                paginationClass: 'pagination',
            },
        };
        return new List('container', options);
    }

    function refilter() {
        function dataCentreFilter(item) {
            return state.centre === "All" || state.centre === item.values().centre;
        }

        function roleFilter(item) {
            if (Number(item.elm.dataset.numParties) !== 1) {
                return true;
            }
            return state.roles === 0n || state.roles & BigInt(item.elm.dataset.joinableRoles);
        }

        function highEndFilter(item) {
            if (!state.highEnd) return true;
            return item.elm.dataset.highEnd === 'true';
        }

        function objectiveFilter(item) {
            if (state.objectives === 0) return true;
            let itemObj = parseInt(item.elm.dataset.objective || '0');
            // OR logic: show if listing matches ANY of the selected objectives
            // But usually for objectives like 'Practice', 'Loot', listings only have one main flag locally,
            // but server sends bitflags.
            // If listing has 'Practice' and I select 'Practice', (itemObj & state.objectives) will be non-zero.
            return (itemObj & state.objectives) !== 0;
        }

        function conditionFilter(item) {
            // Conditions (Duty Complete, etc)
            if (state.conditions !== 0) {
                let itemCond = parseInt(item.elm.dataset.conditions || '0');
                if ((itemCond & state.conditions) === 0) return false;
            }

            // One Player Per Job (Search Area flag 32)
            if (state.onePlayerPerJob) {
                let itemSearchArea = parseInt(item.elm.dataset.searchArea || '0');
                // 32 = 1<<5
                if ((itemSearchArea & 32) === 0) return false;
            }

            return true;
        }

        function minItemLevelFilter(item) {
            if (!state.minItemLevel || state.minItemLevel <= 0) return true;
            let itemLevel = parseInt(item.elm.dataset.minItemLevel || '0');
            return itemLevel >= state.minItemLevel;
        }

        // 콘텐츠 타입 필터 (Ultimate=28, Savage=5, Extreme=4, Chaotic=37)
        function contentTypeFilter(item) {
            if (state.contentTypes.length === 0) return true;
            let contentKind = parseInt(item.elm.dataset.contentKind || '0');
            return state.contentTypes.includes(contentKind);
        }

        // 선택된 콘텐츠 필터 (duty name 배열 기반)
        function selectedContentsFilter(item) {
            if (state.selectedContents.length === 0) return true;
            let dutyName = item.elm.querySelector('.duty');
            if (!dutyName) return true;
            return state.selectedContents.includes(dutyName.textContent);
        }

        state.list.filter(item =>
            dataCentreFilter(item) &&
            roleFilter(item) &&
            highEndFilter(item) &&
            objectiveFilter(item) &&
            conditionFilter(item) &&
            minItemLevelFilter(item) &&
            contentTypeFilter(item) &&
            selectedContentsFilter(item)
        );

        // 필터 결과가 없을 때 메시지 표시
        const noFilterResults = document.getElementById('filter-no-listings');
        const serverNoListings = document.querySelector('.no-listings:not(#filter-no-listings)');

        // 서버에 리스팅이 아예 없는 경우는 HTML에서 처리됨 (listings.html)
        // 여기서는 "서버에는 있는데 필터로 다 가려진 경우"를 처리
        // state.list.visibleItems.length === 0 이고 state.list.items.length > 0 일 때
        if (state.list.visibleItems.length === 0 && state.list.items.length > 0) {
            if (noFilterResults) noFilterResults.style.display = 'block';
        } else {
            if (noFilterResults) noFilterResults.style.display = 'none';
        }
    }

    function setUpDataCentreFilter() {
        let select = document.getElementById('data-centre-filter');

        let dataCentres = {};
        for (let item of state.list.items) {
            // High-end 필터가 켜져있으면(항상 true), High-end가 아닌 것은 카운트에서 제외
            if (state.highEnd && item.elm.dataset.highEnd !== 'true') {
                continue;
            }

            let centre = item.values().centre;
            if (!dataCentres.hasOwnProperty(centre)) {
                dataCentres[centre] = 0;
            }

            dataCentres[centre] += 1;
        }

        for (let opt of select.options) {
            let centre = opt.value;

            let count = 0;

            if (dataCentres.hasOwnProperty(centre)) {
                count = dataCentres[centre];
            }

            if (centre === 'All') {
                count = Object.values(dataCentres).reduce((a, b) => a + b, 0);
            }

            opt.innerText += ` (${count})`;
        }

        select.addEventListener('change', () => {
            state.centre = select.value;
            refilter();
        });
    }



    function setUpRoleFilter() {
        let select = document.getElementById('role-filter');

        select.addEventListener('change', (event) => {
            let value = BigInt(event.target.value);
            if (event.target.checked) {
                state.roles |= value;
            } else {
                state.roles &= ~value;
            }
            refilter();
        });
    }

    function setUpAdvancedFilters() {
        // High-end 필터는 항상 활성화됨 (버튼 제거)
        state.highEnd = true;

        // Objective
        const objFilter = document.getElementById('objective-filter');
        objFilter.addEventListener('change', (e) => {
            const val = parseInt(e.target.value);
            if (e.target.checked) {
                state.objectives |= val;
            } else {
                state.objectives &= ~val;
            }
            refilter();
        });

        // Condition
        const condFilter = document.getElementById('condition-filter');
        condFilter.addEventListener('change', (e) => {
            const val = parseInt(e.target.value);
            if (val === 32) {
                state.onePlayerPerJob = e.target.checked;
            } else {
                if (e.target.checked) {
                    state.conditions |= val;
                } else {
                    state.conditions &= ~val;
                }
            }
            refilter();
        });

        // 콘텐츠 타입 필터 (Ultimate, Savage, Extreme, Chaotic)
        const contentTypeFilter = document.getElementById('content-type-filter');
        if (contentTypeFilter) {
            contentTypeFilter.addEventListener('change', (e) => {
                const val = parseInt(e.target.value);
                if (e.target.checked) {
                    if (!state.contentTypes.includes(val)) {
                        state.contentTypes.push(val);
                    }
                } else {
                    state.contentTypes = state.contentTypes.filter(v => v !== val);
                }
                refilter();
            });
        }

        // 콘텐츠 멀티셀렉트 드롭다운 설정
        setupContentSelect();
    }

    // 콘텐츠 멀티셀렉트 드롭다운
    function setupContentSelect() {
        const trigger = document.getElementById('content-select-trigger');
        const container = trigger?.closest('.content-select-container');
        const dropdown = document.getElementById('content-select-dropdown');
        const listEl = document.getElementById('content-select-list');
        const selectedEl = document.getElementById('selected-contents');

        if (!trigger || !container || !dropdown || !listEl) return;

        // 모든 리스팅에서 고유한 duty 이름 수집 (필터로 숨겨진 것도 포함)
        function collectDutyNames() {
            const names = new Set();
            // 제외할 이름들 (None 등)
            const excludeNames = ['None', '設定なし', 'Nicht festgelegt', 'Non spécifiée'];
            // state.list의 모든 items에서 수집 (필터 상태 무관)
            if (state.list && state.list.items) {
                state.list.items.forEach(item => {
                    const dutyEl = item.elm.querySelector('.duty');
                    if (dutyEl && dutyEl.textContent && !excludeNames.includes(dutyEl.textContent)) {
                        names.add(dutyEl.textContent);
                    }
                });
            }
            return Array.from(names).sort();
        }

        // 드롭다운 리스트 생성
        function populateList() {
            listEl.innerHTML = '';
            const dutyNames = collectDutyNames();
            dutyNames.forEach(name => {
                const item = document.createElement('label');
                item.className = 'content-select-item';
                item.innerHTML = `
                    <input type="checkbox" value="${name}" ${state.selectedContents.includes(name) ? 'checked' : ''}>
                    <span>${name}</span>
                `;
                listEl.appendChild(item);
            });
        }

        // 선택된 콘텐츠 태그 렌더링
        function renderSelectedTags() {
            selectedEl.innerHTML = '';
            state.selectedContents.forEach(name => {
                const tag = document.createElement('span');
                tag.className = 'selected-content-tag';
                tag.innerHTML = `${name}<span class="remove-btn">✕</span>`;
                tag.querySelector('.remove-btn').addEventListener('click', () => {
                    state.selectedContents = state.selectedContents.filter(n => n !== name);
                    renderSelectedTags();
                    refilter();
                    // 체크박스 동기화
                    const checkbox = listEl.querySelector(`input[value="${name}"]`);
                    if (checkbox) checkbox.checked = false;
                });
                selectedEl.appendChild(tag);
            });

            // 트리거 텍스트 업데이트
            const triggerText = trigger.querySelector('span');
            if (state.selectedContents.length > 0) {
                triggerText.textContent = `${state.selectedContents.length} selected`;
            } else {
                triggerText.textContent = TRANSLATIONS.select_content?.[state.lang] || 'Select Content...';
            }
        }

        // 드롭다운 토글
        trigger.addEventListener('click', () => {
            const isOpen = container.classList.toggle('open');
            if (isOpen) {
                populateList();
            }
        });

        // 외부 클릭 시 닫기
        document.addEventListener('click', (e) => {
            if (!container.contains(e.target)) {
                container.classList.remove('open');
            }
        });

        // 체크박스 변경
        listEl.addEventListener('change', (e) => {
            if (e.target.type === 'checkbox') {
                const name = e.target.value;
                if (e.target.checked) {
                    if (!state.selectedContents.includes(name)) {
                        state.selectedContents.push(name);
                    }
                } else {
                    state.selectedContents = state.selectedContents.filter(n => n !== name);
                }
                renderSelectedTags();
                refilter();
            }
        });

        // 초기 렌더링
        renderSelectedTags();
    }


    function setupPaginationNav() {
        let prev = document.querySelector('.page-btn.prev');
        let next = document.querySelector('.page-btn.next');

        if (!prev || !next) return;

        function updateButtons() {
            let list = state.list;
            let i = list.i || 1;
            let page = list.page;

            // 첫 페이지면 prev 비활성화
            if (i <= 1) {
                prev.classList.add('disabled');
            } else {
                prev.classList.remove('disabled');
            }

            // 마지막 페이지면 next 비활성화
            if (i + page > list.size()) {
                next.classList.add('disabled');
            } else {
                next.classList.remove('disabled');
            }

            // '...' 항목 비활성화 (List.js가 렌더링한 후)
            setTimeout(() => {
                let paginationLinks = document.querySelectorAll('.pagination li a');
                paginationLinks.forEach(a => {
                    if (a.innerText === '...') {
                        a.parentElement.classList.add('disabled');
                    }
                });
            }, 0);
        }

        prev.addEventListener('click', (e) => {
            e.preventDefault();
            let list = state.list;
            let i = (list.i || 1) - list.page;
            if (i < 1) i = 1;
            list.show(i, list.page);
        });

        next.addEventListener('click', (e) => {
            e.preventDefault();
            let list = state.list;
            let i = (list.i || 1) + list.page;
            if (i > list.size()) return;
            list.show(i, list.page);
        });

        state.list.on('updated', updateButtons);
        updateButtons();
    }

    addJsClass();
    saveLoadState();
    // Translations handled by translations.js


    function applyTranslations() {
        const lang = state.lang || 'en';
        document.querySelectorAll('[data-i18n]').forEach(elem => {
            const key = elem.dataset.i18n;
            if (TRANSLATIONS[key] && TRANSLATIONS[key][lang]) {
                elem.textContent = TRANSLATIONS[key][lang];
            }
        });
    }

    // 시간 표시 i18n 함수
    function formatRelativeTime(seconds, lang) {
        const absSeconds = Math.abs(seconds);
        const isFuture = seconds > 0;

        if (absSeconds < 60) {
            return TRANSLATIONS.time_now[lang] || 'now';
        }

        let value, unitKey;
        if (absSeconds < 3600) {
            value = Math.floor(absSeconds / 60);
            unitKey = value === 1 ? 'time_minute' : 'time_minutes';
        } else {
            value = Math.floor(absSeconds / 3600);
            unitKey = value === 1 ? 'time_hour' : 'time_hours';
        }

        const unit = TRANSLATIONS[unitKey] ? TRANSLATIONS[unitKey][lang] : unitKey.split('_')[1];

        // 일본어는 "5分後" / "5分前" 형식
        if (lang === 'ja') {
            const suffix = isFuture ? TRANSLATIONS.time_in[lang] : TRANSLATIONS.time_ago[lang];
            return `${value}${unit}${suffix}`;
        }
        // 독일어 "vor 5 Minuten" / "in 5 Minuten" 형식
        else if (lang === 'de') {
            if (isFuture) {
                return `${TRANSLATIONS.time_in[lang]} ${value} ${unit}`;
            } else {
                return `${TRANSLATIONS.time_ago[lang]} ${value} ${unit}`;
            }
        }
        // 영어/프랑스어 "in 5 minutes" / "5 minutes ago"
        else {
            if (isFuture) {
                return `${TRANSLATIONS.time_in[lang] || 'in'} ${value} ${unit}`;
            } else {
                return `${value} ${unit} ${TRANSLATIONS.time_ago[lang] || 'ago'}`;
            }
        }
    }

    // 절대 시간 포맷 (툴팁용)
    function formatAbsoluteTime(timestamp, lang) {
        const date = new Date(timestamp * 1000);
        const options = { hour: '2-digit', minute: '2-digit' };
        return date.toLocaleTimeString(lang === 'ja' ? 'ja-JP' : lang === 'de' ? 'de-DE' : lang === 'fr' ? 'fr-FR' : 'en-US', options);
    }

    // 시간 표시 업데이트
    function updateTimeDisplays() {
        const lang = state.lang || 'en';
        const now = Math.floor(Date.now() / 1000);

        document.querySelectorAll('.item.expires[data-expires-in]').forEach(elem => {
            const expiresIn = parseInt(elem.dataset.expiresIn);
            const textSpan = elem.querySelector('.text');
            if (textSpan && !isNaN(expiresIn)) {
                textSpan.textContent = formatRelativeTime(expiresIn, lang);
                // 툴팁: 만료 예정 절대 시간
                const expiresAt = now + expiresIn;
                const label = TRANSLATIONS.expires_at ? TRANSLATIONS.expires_at[lang] : 'Expires at';
                elem.title = `${label}: ${formatAbsoluteTime(expiresAt, lang)}`;
            }
        });

        document.querySelectorAll('.item.updated[data-updated-at]').forEach(elem => {
            const updatedAt = parseInt(elem.dataset.updatedAt);
            const textSpan = elem.querySelector('.text');
            if (textSpan && !isNaN(updatedAt)) {
                const secondsAgo = now - updatedAt;
                textSpan.textContent = formatRelativeTime(-secondsAgo, lang);
                // 툴팁: 업데이트 절대 시간
                const label = TRANSLATIONS.updated_at ? TRANSLATIONS.updated_at[lang] : 'Updated at';
                elem.title = `${label}: ${formatAbsoluteTime(updatedAt, lang)}`;
            }
        });
    }

    // Scroll to Top 버튼 설정
    function setupScrollToTop() {
        const btn = document.getElementById('scroll-to-top');
        if (!btn) return;

        // 스크롤 위치에 따라 버튼 표시/숨기기
        window.addEventListener('scroll', () => {
            if (window.scrollY > 300) {
                btn.classList.add('visible');
            } else {
                btn.classList.remove('visible');
            }
        });

        // 클릭 시 상단으로 스크롤
        btn.addEventListener('click', () => {
            window.scrollTo({ top: 0, behavior: 'smooth' });
        });
    }

    reflectState();
    state.list = setUpList();
    setUpDataCentreFilter();
    setUpRoleFilter();
    setUpAdvancedFilters();
    applyTranslations(); // Apply translations on load
    updateTimeDisplays(); // 시간 표시 i18n 적용
    refilter();
    setupPaginationNav();
    setupScrollToTop();

    // 1분마다 시간 표시 업데이트
    setInterval(updateTimeDisplays, 60000);
})();
