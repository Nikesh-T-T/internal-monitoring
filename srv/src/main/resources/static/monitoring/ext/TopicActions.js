sap.ui.define([
	"sap/m/Dialog",
	"sap/m/Button",
	"sap/m/Select",
	"sap/ui/core/Item",
	"sap/m/Label",
	"sap/m/VBox",
	"sap/m/MessageToast",
	"sap/m/MessageBox",
	"sap/base/Log"
], function (Dialog, Button, Select, Item, Label, VBox, MessageToast, MessageBox, Log) {
	"use strict";

	var SERVICE_URL = "/odata/v4/MonitoringService/";

	function readStatus() {
		return fetch(SERVICE_URL + "IngestionStatus", {
			headers: { Accept: "application/json" }
		}).then(function (response) {
			if (!response.ok) {
				throw new Error("HTTP " + response.status);
			}
			return response.json();
		}).then(function (body) {
			return (body.value && body.value[0]) || null;
		});
	}

	function callAction(name, payload) {
		return fetch(SERVICE_URL + name, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				Accept: "application/json"
			},
			body: JSON.stringify(payload)
		}).then(function (response) {
			return response.json().then(function (body) {
				if (!response.ok) {
					throw new Error((body.error && body.error.message) || "HTTP " + response.status);
				}
				return body.value;
			});
		});
	}

	/**
	 * Returns the app to a clean list view after a topic switch.
	 *
	 * The switch purges the previous topic's rows on the server and the poller
	 * re-fills the new topic asynchronously. A plain model refresh is unsafe here:
	 * the flexible column layout may still hold an open object page bound to a row
	 * that was just purged, so refreshing re-reads it and fails with a 404. Rather
	 * than race the routing/binding lifecycle, we drop the hash back to the bare
	 * list route and reload the app, which rebuilds every binding from the server
	 * and guarantees no stale detail context or leftover rows survive.
	 */
	function refreshList() {
		try {
			var hash = window.location.hash || "";
			var base = hash.split("&/")[0]; // strip any FCL detail segment
			window.location.hash = base || "#";
		} catch (error) {
			Log.warning("Could not reset the route after topic switch: " + error.message, null, "monitoring");
		}
		window.location.reload();
	}

	function openDialog(status) {
		var topics = status.selectableTopics || [];
		var active = status.activeTopic;

		var select = new Select({ width: "100%" });
		topics.forEach(function (topic) {
			select.addItem(new Item({ key: topic, text: topic }));
		});
		if (active) {
			select.setSelectedKey(active);
		}

		var dialog = new Dialog({
			title: "Monitored Topic",
			contentWidth: "28rem",
			content: [
				new VBox({
					renderType: "Bare",
					items: [
						new Label({ text: "Kafka topic", labelFor: select }),
						select
					]
				}).addStyleClass("sapUiSmallMargin")
			],
			beginButton: new Button({
				text: "Switch",
				type: "Emphasized",
				press: function () {
					var chosen = select.getSelectedKey();
					if (!chosen || chosen === active) {
						dialog.close();
						return;
					}
					MessageBox.confirm(
						"Switching to '" + chosen + "' deletes all messages collected for '"
							+ active + "'. Continue?",
						{
							onClose: function (action) {
								if (action !== MessageBox.Action.OK) {
									return;
								}
								dialog.setBusy(true);
								callAction("setTopic", { topic: chosen }).then(function (result) {
									dialog.setBusy(false);
									dialog.close();
									MessageToast.show(result || "Topic switched.");
									refreshList();
								}).catch(function (error) {
									dialog.setBusy(false);
									MessageBox.error(error.message);
								});
							}
						}
					);
				}
			}),
			endButton: new Button({
				text: "Cancel",
				press: function () {
					dialog.close();
				}
			}),
			afterClose: function () {
				dialog.destroy();
			}
		});

		dialog.open();
	}

	/**
	 * Custom Fiori elements action. Opens a dialog listing the selectable topics
	 * with the currently monitored one preselected, then calls the backend
	 * `setTopic` action which re-points the poller and purges the old topic.
	 */
	return {
		openTopicSwitcher: function () {
			readStatus().then(function (status) {
				if (!status || !(status.selectableTopics && status.selectableTopics.length)) {
					MessageToast.show("No selectable topics are configured.");
					return;
				}
				openDialog(status);
			}).catch(function (error) {
				MessageBox.error("Could not read the current topic: " + error.message);
			});
		}
	};
});
