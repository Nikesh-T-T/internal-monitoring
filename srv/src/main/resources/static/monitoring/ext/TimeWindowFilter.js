sap.ui.define([
	"sap/ui/model/Filter",
	"sap/ui/model/FilterOperator",
	"sap/ui/core/Element"
], function (Filter, FilterOperator, Element) {
	"use strict";

	/**
	 * Preset time-window filter for the Messages list. FE custom filter fragments
	 * don't reliably bind a core:require event handler via change="...", so the
	 * module self-attaches its change listener to the Select once it exists. On
	 * change it applies an "Application"-type filter directly on the table's
	 * ODataListBinding, which AND-combines with the FilterBar's own ("Control")
	 * filters and forces a server request with $filter=messageTimestamp ge
	 * (now - N minutes).
	 */
	function findTable(control) {
		var view = control;
		while (view && !(view.isA && view.isA("sap.ui.core.mvc.View"))) {
			view = view.getParent();
		}
		if (!view) {
			return null;
		}
		return view.findAggregatedObjects(true, function (candidate) {
			return candidate.isA && candidate.isA("sap.ui.mdc.Table");
		})[0];
	}

	function onChange(event) {
		var key = event.getParameter("selectedItem").getKey();
		var minutes = key ? parseInt(key, 10) : 0;

		var table = findTable(event.getSource());
		var binding = table && table.getRowBinding && table.getRowBinding();
		if (!binding) {
			return;
		}

		if (!minutes) {
			binding.filter([], "Application");
			return;
		}
		var since = new Date(Date.now() - minutes * 60000).toISOString();
		binding.filter(new Filter({
			path: "messageTimestamp",
			operator: FilterOperator.GE,
			value1: since
		}), "Application");
	}

	function findSelect() {
		var found = null;
		Element.registry.forEach(function (el) {
			if (found) {
				return;
			}
			if (el.isA && el.isA("sap.m.Select") &&
				/timeWindowSelect$/.test(el.getId()) &&
				!el.data("twAttached")) {
				found = el;
			}
		});
		return found;
	}

	function attachWhenReady(attempts) {
		var remaining = typeof attempts === "number" ? attempts : 40;
		var select = findSelect();
		if (select) {
			select.attachChange(onChange);
			select.data("twAttached", true);
			return;
		}
		if (remaining > 0) {
			setTimeout(function () {
				attachWhenReady(remaining - 1);
			}, 500);
		}
	}

	attachWhenReady();

	return {
		onChange: onChange
	};
});
