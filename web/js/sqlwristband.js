/* SqlWristband | (c) 2014-2015 Andrey Shevtsov | Licensed under GPL version 2 */
var plots = []; // array of plots
var allData = []; // array of data points
var allSeries = []; // array of series names
var MIN_CHART_HEIGHT = 200; // minimum height of a chart in pixels
var refreshInterval = 60;
var refreshCountdown = refreshInterval - 1;
var refreshFunction = "";
var autoRefreshOn = true;
var activeModal = ""; // Id of active modal window. messageBox function will close it before showing up intself (Bootstrap 3 does not support overlapping modals)

plotTheme = {
	grid: {
		backgroundColor: '#fcfcfc'
	},
	legend: {
		location: 'n'
	},
	axesDefaults: {
		useSeriesColor: true,
		rendererOptions: {
			alignTicks: true
		}
	}
}

window.onresize = function(event) {
	set_chart_div_height(plots);
	for (var div in plots) {
		plots[div].chart.replot( { resetAxes: true } );
	}
};

function set_chart_div_height(plots, totalMargin) {
	totalMargin = typeof totalMargin !== 'undefined' ? totalMargin : 0;
	numberOfPlots = Object.keys(plots).length;
	windowHeight = $(window).height() - $('#main-area').offset().top - $('#footer').outerHeight();

	for (var div in plots) {
		var height = plots[div].heightPct*windowHeight/100 - (35 + totalMargin)/numberOfPlots;
		if (height < MIN_CHART_HEIGHT)
			height = MIN_CHART_HEIGHT;

		$('#' + div).height(height);
	}
}

function enable_multi_plot_zooming() {
	// multi-plot zooming
	$.jqplot.Cursor.prototype.doZoomOrig = $.jqplot.Cursor.prototype.doZoom;
	$.jqplot.Cursor.prototype.doZoom = function (gridpos, datapos, plot, cursor) {
		for (var div in plots) {
			$.jqplot.Cursor.prototype.doZoomOrig(gridpos, datapos, plots[div].chart, cursor);
		}
	}

	// multi-plot zoom reset
	$.jqplot.Cursor.prototype.resetZoomOrig = $.jqplot.Cursor.prototype.resetZoom;
	$.jqplot.Cursor.prototype.resetZoom = function(plot, cursor) {
		for (var div in plots) {
			plots[div].chart.resetZoom(); 
		}
	}
}

function getURLParameter(name) {
	return decodeURIComponent((new RegExp('[?|&]' + name + '=' + '([^&;]+?)(&|#|;|$)').exec(location.search)||[,""])[1].replace(/\+/g, '%20'))||null;
}

function requestDataLoad(url, cbSuccess) {
	callWebService('LoadData', 'GET', url, '', true, function (data) {
		cbSuccess(data);
	});
}

function formatDate(dt) {
	var strDate = '' + dt.getFullYear();

	if (dt.getMonth()+1 < 10)
		strDate = strDate + '0' + (dt.getMonth()+1);
	else
		strDate = strDate + (dt.getMonth()+1);

	if (dt.getDate() < 10)
		strDate = strDate + '0' + dt.getDate();
	else
		strDate = strDate + dt.getDate();

	if (dt.getHours() < 10)
		strDate = strDate + '0' + dt.getHours();
	else
		strDate = strDate + dt.getHours();

	if (dt.getMinutes() < 10)
		strDate = strDate + '0' + dt.getMinutes();
	else
		strDate = strDate + dt.getMinutes();

	return strDate;
}

function addCheckboxLiToUl(ulId, cssClass, value, isChecked, text)
{
	// <li><input class="legendVis" value="cxpacket" type="checkbox" checked="checked" /><span>CXPACKET</span></li>
	var ul = document.getElementById(ulId);

	var li = document.createElement("li");
	var input = document.createElement("input");
	var span = document.createElement("span");

	input.setAttribute('class', cssClass);
	input.setAttribute('type', 'checkbox');
	input.setAttribute('value', value);

	if (isChecked)
		input.setAttribute('checked', 'checked');

	span.textContent = text;

	li.appendChild(input);
	li.appendChild(span);
	ul.appendChild(li);
}

function resetCountdown() {
	refreshCountdown = refreshInterval;
}

// Make sure you have span element with id refreshCountdown
function countdown(refreshFunc) {
	refreshFunction = refreshFunc;
	if (!autoRefreshOn)
		return;

	if (refreshCountdown <= 0) {
		resetCountdown();
		refreshFunction();
		setTimeout(function(){ countdown(refreshFunction); }, 1000);
	} else {
		refreshCountdown = refreshCountdown - 1;
		if (refreshCountdown < 10)
			countdownText = '0' + refreshCountdown;
		else
			countdownText = '' + refreshCountdown;

		$('#refreshCountdown').text(countdownText);
		setTimeout(function(){ countdown(refreshFunction); }, 1000);
	}
}

function toggleRefresh() {
	autoRefreshOn = !autoRefreshOn;
	$('#refreshCountdown').text('Off');
	if (autoRefreshOn)
		countdown(refreshFunction);
}

// Shows message to user (error, info)
function messageBox(msg, title) {
	// Close currently active modal window
	if (activeModal != '') {
		// Do not close another message box
		if (activeModal == 'messageBox')
			return;

		$('#' + activeModal).modal('hide');
	}

	$('#messageBoxLabel').text(title);

	$('#messageBoxText').text(msg);

	$('#messageBox').modal('show');
	activeModal = 'messageBox';
}

function callWebService(callname, type, url, data, isJson, callback)
{
	$.ajax({
		async: true,
		type : type,
		url  : url,
		data : data,
		dataType: 'json',
		success : function (data) {
			if (data.Status != "Success") {
				messageBox(Base64.decode(data.Message), 'Error');
			} else {
				if (isJson)
					realData = eval('(' + Base64.decode(data.Message) + ')');
				else if (data.hasOwnProperty('Message'))
					realData = Base64.decode(data.Message);
				else
					realData = '';

				callback(realData);
			}
		},
		error   : function (jqXHR, textStatus, errorThrown) {
			console.log(jqXHR);
			messageBox('AJAX call ' + callname + ' failed with error: ' + errorThrown, 'Error');
		}
	});	
}
