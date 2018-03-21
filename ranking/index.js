$(function() {
    let allTasksObject;
    let allTasksArray;
    let showControls;
    let controlsMode;

    function init() {
        initAllTasks();
        showControls = needsControls();
        setControlsVisibility(showControls);
        initControls();
        refresh();
    }

    function initAllTasks() {
        allTasksObject = {};
        allTasksArray = [];
        let contests = raw_data.contests;
        for(let i = 0; i < contests.length; i++) {
            let tasks = contests[i].tasks;
            for(let j = 0; j < tasks.length; j++) {
                let taskName = tasks[j];
                allTasksObject[taskName] = true;
                allTasksArray.push(taskName);
            }
        }
    }

    function isHomeName(taskName) {
        return taskName.startsWith(".");
    }

    function getHomeName(taskName) {
        return "." + taskName;
    }

    function getClassroomName(taskName) {
        if(!isHomeName(taskName)) {
            return null;
        }
        return taskName.substring(1);
    }

    /*
     * If both "task1" and ".task1" exist, we need controls.
     * This function checks whether this is the case.
     */
    function needsControls() {
        for(let taskName in allTasksObject) {
            if(allTasksObject[getHomeName(taskName)]) {
                return true;
            }
        }
        return false;
    }

    function setControlsVisibility(visible) {
        var value;
        if(visible) {
            value = "block";
        }
        else {
            value = "none";
        }
        $("#controls").css("display", value);
    }

    function initControls() {
        $("#controlSelect").on("change", onControlsChange);
        controlsMode = "best";
    }

    function onControlsChange() {
        controlsMode = this.value;
        refresh();
    }

    let taskColumns;
    let userList;

    function refresh() {
        refreshTasks();
        refreshUserList();
        refreshHeader();
        addRows();
    }

    function refreshTasks() {
        /*
         * The task columns are all relevant tasks,
         * according to the selection.
         * Home tasks are not relevant when "onlyClassroom" or "best".
         */
        taskColumns = [];
        for(let i = 0; i < allTasksArray.length; i++) {
            let taskName = allTasksArray[i];
            if(controlsMode != "onlyHome" && isHomeName(taskName)) {
                continue;
            }
            if(controlsMode == "onlyHome" && !isHomeName(taskName)) {
                continue;
            }
            taskColumns.push(taskName);
        }
    }

    function scoreStringToNumber(scoreString) {
        if(scoreString === null || scoreString === undefined || scoreString == "") {
            return 0;
        }
        if(scoreString.endsWith("*")) {
            scoreString = scoreString.substring(0, scoreString.length - 1);
        }
        return parseFloat(scoreString);
    }

    function getMaxScoreString(scoreString1, scoreString2) {
        if(scoreString1 === scoreString2) {
            return scoreString1;
        }
        if(scoreString1 === null || scoreString1 === undefined) {
            return scoreString2;
        }
        if(scoreString2 === null || scoreString2 === undefined) {
            return scoreString1;
        }

        var score1 = scoreStringToNumber(scoreString1);
        var score2 = scoreStringToNumber(scoreString2);
        if(score1 < score2) {
            return scoreString2;
        }
        else if(score2 < score1) {
            return scoreString1;
        }

        if(scoreString1.endsWith("*")) {
            return scoreString1;
        }

        return scoreString2;
    }

    function refreshUserList() {
        userList = [];
        for(let user in raw_data.scores) {
            userInfo = {
                user: user,
                scores: {},
                globalScore: 0,
                tasksAttempted: 0
            };

            for(let i = 0; i < taskColumns.length; i++) {
                // For each task, we take into account both its score
                // and the home score.
                let taskName = taskColumns[i];
                let scoreString = raw_data.scores[user][taskName];
                let score = scoreStringToNumber(scoreString);
                let homeName = getHomeName(taskName);
                let homeScoreString = raw_data.scores[user][homeName];
                let homeScore = scoreStringToNumber(homeScoreString);

                // If neither are present, skip.
                if(scoreString === undefined && homeScoreString === undefined) {
                    continue;
                }

                // One of the scores is present: original, or home.
                // If the original isn't present, and the mode isn't best
                // of both, then skip.
                if(scoreString === undefined && controlsMode !== "best") {
                    continue;
                }

                // This task counts as attempted.
                userInfo.tasksAttempted++;

                // If the mode is best of both, and the home improves the score,
                // we take it. Otherwise, use the original score.
                if(controlsMode === "best") {
                    userInfo.scores[taskName] = getMaxScoreString(scoreString, homeScoreString);
                    userInfo.globalScore += Math.max(score, homeScore);
                }
                else {
                    userInfo.scores[taskName] = scoreString;
                    userInfo.globalScore += score;
                }
            }
            userList.push(userInfo);
        }

        userList.sort(userComparator);
    }

    function userComparator(userInfo1, userInfo2) {
        if(userInfo1.globalScore != userInfo2.globalScore) {
            return userInfo2.globalScore - userInfo1.globalScore;
        }
        if(userInfo1.tasksAttempted != userInfo2.tasksAttempted) {
            return userInfo2.tasksAttempted - userInfo1.tasksAttempted;
        }
        return userInfo1.user.localeCompare(userInfo2.user);
    }

    function refreshHeader() {
        let html = "<tr class=\"headerRow\"><td class=\"headerRow username\"></td>";
        for(let i = 0; i < taskColumns.length; i++) {
            let taskName = taskColumns[i];
            html += "<td class=\"taskHeader\" title=\"" + taskName + "\">" + taskName + "</td>";
        }
        html += "<td class=\"globalSeparator\"></td>";
        html += "<td class=\"taskHeader globalScore\">Global</td></tr>";
        $(".mainTable").html(html);
    }

    function scoreStringToClass(scoreString, maximum) {
        if(scoreString === null || scoreString === undefined || scoreString === "") {
            return "score_none";
        }

        let score = scoreStringToNumber(scoreString);
        let percentage = Math.round((1.0 * score) / maximum * 100.0);
        if(percentage <= 0) {
            return "score_0";
        }
        if(percentage >= 100) {
            return "score_100";
        }
        let chunk = Math.floor(percentage / 10) * 10;
        return "score_" + chunk + "_" + (chunk + 10);
    }

    function scoreToDisplay(scoreString) {
        if(scoreString === null || scoreString === undefined || scoreString === "") {
            return "-";
        }

        // If the score is very very close to the previous integer, we round.
        // Makes "100.0" show as "100".
        let partial = scoreString.endsWith("*");
        let score = scoreStringToNumber(scoreString);
        let scoreInt = Math.floor(score);

        if(Math.abs(score - scoreInt) < 0.00001) {
            scoreString = String(scoreInt);
            if(partial) {
                scoreString += "*";
            }
        }

        return scoreString;
    }

    function addRows() {
        let html = "";
        for(let i = 0; i < userList.length; i++) {
            let userInfo = userList[i];
            let user = userInfo.user;
            html += "<tr class=\"userRow\">";
            html += "<td class=\"username\" title=\"" + user + "\">" + user + "</td>";
            for(let j = 0; j < taskColumns.length; j++) {
                let taskName = taskColumns[j];
                let scoreString = userInfo.scores[taskName];
                let displayScore = scoreToDisplay(scoreString);
                let scoreClass = scoreStringToClass(scoreString, 100);
                html += "<td class=\"taskScore " + scoreClass + "\">" + displayScore + "</td>";
            }

            let scoreString = String(userInfo.globalScore);
            let displayScore = scoreToDisplay(scoreString);
            let scoreClass = scoreStringToClass(scoreString, 100 * taskColumns.length);
            html += "<td class=\"globalSeparator\"></td>";
            html += "<td class=\"taskScore " + scoreClass + "\">" + displayScore + "</td>";
            html += "</tr>";
        }
        $(".mainTable").append(html);
    }

    init();
});
