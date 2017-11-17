/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {browser, element, by, protractor} from 'protractor';
import {waitForElementVisibility, waitForElementPresence, waitForElementInVisibility} from '../utils/e2e_util';

export class MetronAlertsPage {
  navigateTo() {
    browser.waitForAngularEnabled(false);
    return browser.get('/alerts-list');
  }

  clearLocalStorage() {
    browser.executeScript('window.localStorage.clear();');
  }

  isMetronLogoPresent() {
    return element(by.css('img[src="../assets/images/logo.png"]')).isPresent();
  }

  isSavedSearchButtonPresent() {
    return element(by.buttonText('Searches')).isPresent();
  }

  isClearSearchPresent() {
    return element(by.css('.btn-search-clear')).isPresent();
  }

  isSearchButtonPresent() {
    return element(by.css('.btn-search-clear')).isPresent();
  }

  isSaveSearchButtonPresent() {
    return element(by.css('.save-button')).isPresent();
  }

  isTableSettingsButtonPresent() {
    return element(by.css('.btn.settings')).isPresent();
  }

  isPausePlayRefreshButtonPresent() {
    return element(by.css('.btn.pause-play')).isPresent();
  }

  isActionsButtonPresent() {
    return element.all(by.buttonText('ACTIONS')).isPresent();
  }

  isConfigureTableColumnsPresent() {
    return element(by.css('.fa.fa-cog.configure-table-icon')).isPresent();
  }

  getAlertTableTitle() {
    return element(by.css('.col-form-label-lg')).getText();
  }

  clickActionDropdown() {
    let actionsDropDown = element(by.buttonText('ACTIONS'));
    browser.actions().mouseMove(actionsDropDown).perform();
    return actionsDropDown.click();
  }

  clickActionDropdownOption(option: string) {
    this.clickActionDropdown().then(() => {
      element(by.cssContainingText('.dropdown-menu span', option)).click();
      browser.sleep(2000);
    });
  }

  getActionDropdownItems() {
    return this.clickActionDropdown().then(() => element.all(by.css('.dropdown-menu .dropdown-item.disabled')).getText());
  }

  getTableColumnNames() {
    return element.all(by.css('app-alerts-list .table th')).getText();
  }

  getPaginationText() {
    return element(by.css('metron-table-pagination span')).getText();
  }

  isChevronLeftEnabled() {
    return element(by.css('metron-table-pagination .fa.fa-chevron-left')).getAttribute('class').then((classes) => {
      return classes.split(' ').indexOf('disabled') === -1;
    });
  }

  isChevronRightEnabled() {
    return element(by.css('metron-table-pagination .fa.fa-chevron-right')).getAttribute('class').then((classes) => {
      return classes.split(' ').indexOf('disabled') === -1;
    });
  }

  clickChevronRight(times = 1) {
    for (let i = 0; i < times; i++) {
      element(by.css('metron-table-pagination .fa.fa-chevron-right')).click();
    }
  }

  clickChevronLeft(times = 1) {
    for (let i = 0; i < times; i++) {
      element(by.css('metron-table-pagination .fa.fa-chevron-left')).click();
    }
  }

  clickSettings() {
    let settingsIcon = element(by.css('.btn.settings'));
    return waitForElementVisibility(settingsIcon).then(() => settingsIcon.click());
  }

  getSettingsLabels() {
    return element.all(by.css('app-configure-rows  form label:not(.switch)')).getText();
  }

  getRefreshRateOptions() {
    return element.all(by.css('.preset-row.refresh-interval .preset-cell')).getText();
  }

  getRefreshRateSelectedOption() {
    return element.all(by.css('.preset-row.refresh-interval .preset-cell.is-active')).getText();
  }

  getPageSizeOptions() {
    return element.all(by.css('.preset-row.page-size .preset-cell')).getText();
  }

  getPageSizeSelectedOption() {
    return element.all(by.css('.preset-row.page-size .preset-cell.is-active')).getText();
  }

  clickRefreshInterval(intervalText: string) {
    return element(by.cssContainingText('.refresh-interval .preset-cell', intervalText)).click();
  }

  clickPageSize(pageSizeText: string) {
    return element.all(by.cssContainingText('.page-size .preset-cell', pageSizeText)).first().click();
  }

  clickConfigureTable() {
    let gearIcon = element(by.css('app-alerts-list .fa.fa-cog.configure-table-icon'));
    waitForElementVisibility(gearIcon).then(() => gearIcon.click());
    browser.sleep(1000);
  }

  clickCloseSavedSearch() {
    element(by.css('app-saved-searches .close-button')).click();
    browser.sleep(2000);
  }

  clickSavedSearch() {
    element(by.buttonText('Searches')).click();
    browser.sleep(1000);
  }

  clickPlayPause() {
    element(by.css('.btn.pause-play')).click();
  }

  clickTableText(name: string) {
    waitForElementPresence(element.all(by.linkText(name))).then(() => element.all(by.linkText(name)).get(0).click());
  }

  clickClearSearch() {
    element(by.css('.btn-search-clear')).click();
  }

  getSavedSearchTitle() {
    return element(by.css('app-saved-searches .form-title')).getText();
  }

  getPlayPauseState() {
    return element(by.css('.btn.pause-play i')).getAttribute('class');
  }

  getSearchText() {
    return element(by.css('.ace_line')).getText();
  }

  isCommentIconPresentInTable() {
    return element.all(by.css('app-table-view .fa.fa-comments-o')).count();
  }

  getRecentSearchOptions() {
    browser.sleep(1000);
    return element(by.linkText('Recent Searches')).element(by.xpath('..')).all(by.css('li')).getText();
  }

  getDefaultRecentSearchValue() {
    browser.sleep(1000);
    return element(by.linkText('Recent Searches')).element(by.xpath('..')).all(by.css('i')).getText();
  }

  getSavedSearchOptions() {
    browser.sleep(1000);
    return element(by.linkText('Saved Searches')).element(by.xpath('..')).all(by.css('li')).getText();
  }

  getDefaultSavedSearchValue() {
    browser.sleep(1000);
    return element(by.linkText('Saved Searches')).element(by.xpath('..')).all(by.css('i')).getText();
  }

  getSelectedColumnNames() {
    return element.all(by.css('app-configure-table input[type="checkbox"]:checked')).map(ele => {
      return ele.getAttribute('id').then(id => id.replace(/select-deselect-/, ''));
    });
  }

  toggleSelectCol(name: string, scrollTo = '') {
    scrollTo = scrollTo === '' ? name : scrollTo;
    let ele = element(by.css('app-configure-table label[for="select-deselect-' + name + '"]'));
    let scrollToEle = element(by.css('app-configure-table label[for="select-deselect-' + scrollTo + '"]'));
    browser.actions().mouseMove(scrollToEle).perform().then(() => ele.click());
  }

  saveSearch(name: string) {
     return element(by.css('.save-button')).click().then(() => element(by.css('app-save-search #name')).sendKeys(name))
      .then(() => element(by.css('app-save-search button[type="submit"]')).click());
  }

  saveConfigureColumns() {
    element(by.css('app-configure-table')).element(by.buttonText('SAVE')).click();
  }

  clickRemoveSearchChip() {
    let aceLine = element.all(by.css('.ace_keyword')).get(0);
    /* - Focus on the search text box by sending a empty string
       - move the mouse to the text in search bos so that delete buttons become visible
       - wait for delete buttons become visible
       - click on delete button
    */
    element(by.css('app-alerts-list .ace_text-input')).sendKeys('')
    .then(() => browser.actions().mouseMove(aceLine).perform())
    .then(() => this.waitForElementPresence(element(by.css('.ace_value i'))))
    .then(() => element.all(by.css('.ace_value i')).get(0).click());
  }

  setSearchText(search: string) {
    this.clickClearSearch();
    element(by.css('app-alerts-list .ace_text-input')).sendKeys(protractor.Key.BACK_SPACE);
    element(by.css('app-alerts-list .ace_text-input')).sendKeys(search);
    element(by.css('app-alerts-list .ace_text-input')).sendKeys(protractor.Key.ENTER);
    browser.sleep(2000);
  }

  waitForElementPresence (element ) {
    let EC = protractor.ExpectedConditions;
    return browser.wait(EC.presenceOf(element));
  }

  waitForTextChange(element, previousText) {
    let EC = protractor.ExpectedConditions;
    return browser.wait(EC.not(EC.textToBePresentInElement(element, previousText)));
  }

  toggleAlertInList(index: number) {
    let selector = by.css('app-alerts-list tbody tr label');
    let checkbox = element.all(selector).get(index);
    this.waitForElementPresence(checkbox).then(() => {
      browser.actions().mouseMove(checkbox).perform().then(() => {
        checkbox.click();
      });
    });
  }

  getAlertStatus(rowIndex: number, previousText: string, colIndex = 8) {
    let row = element.all(by.css('app-alerts-list tbody tr')).get(rowIndex);
    let column = row.all(by.css('td a')).get(colIndex);
    return this.waitForTextChange(column, previousText).then(() => {
      return column.getText();
    });
  }

  waitForMetaAlert() {
    browser.sleep(2000);
    return element(by.css('button[data-name="search"]')).click()
    .then(() => waitForElementPresence(element(by.css('.icon-cell.dropdown-cell'))));
  }

  isDateSeettingDisabled() {
    return element.all(by.css('app-time-range button.btn.btn-search[disabled=""]')).count().then((count) => { return (count === 1); });
  }

  clickDateSettings() {
    element(by.css('app-time-range button.btn-search')).click();
    browser.sleep(2000);
  }

  getTimeRangeTitles() {
    return element.all(by.css('app-time-range .title')).getText();
  }

  getQuickTimeRanges() {
    return element.all(by.css('app-time-range .quick-ranges span')).getText();
  }

  getValueForManualTimeRange() {
    return element.all(by.css('app-time-range input.form-control')). getAttribute('value');
  }

  isManulaTimeRangeApplyButtonPresent() {
    return element.all(by.css('app-time-range')).all(by.buttonText('APPLY')).count().then(count => count === 1);
  }

  selectQuickTimeRange(quickRange: string) {
    element.all(by.cssContainingText('.quick-ranges span', quickRange)).get(0).click();
    browser.sleep(2000);
  }

  getTimeRangeButtonText() {
    return element.all(by.css('app-time-range button.btn-search span')).get(0).getText();
  }

  setDate(index: number, year: string, month: string, day: string, hour: string, min: string, sec: string) {
    element.all(by.css('app-time-range .calendar')).get(index).click()
    .then(() => element.all(by.css('.pika-select.pika-select-hour')).get(index).click())
    .then(() => element.all(by.css('.pika-select.pika-select-hour')).get(index).element(by.cssContainingText('option', hour)).click())
    .then(() => element.all(by.css('.pika-select.pika-select-minute')).get(index).click())
    .then(() => element.all(by.css('.pika-select.pika-select-minute')).get(index).element(by.cssContainingText('option', min)).click())
    .then(() => element.all(by.css('.pika-select.pika-select-second')).get(index).click())
    .then(() => element.all(by.css('.pika-select.pika-select-second')).get(index).element(by.cssContainingText('option', sec)).click())
    .then(() => element.all(by.css('.pika-select.pika-select-year')).get(index).click())
    .then(() => element.all(by.css('.pika-select.pika-select-year')).get(index).element(by.cssContainingText('option', year)).click())
    .then(() => element.all(by.css('.pika-select.pika-select-month')).get(index).click())
    .then(() => element.all(by.css('.pika-select.pika-select-month')).get(index).element(by.cssContainingText('option', month)).click())
    .then(() => element.all(by.css('.pika-table')).get(index).element(by.buttonText(day)).click())
    .then(() => waitForElementInVisibility(element.all(by.css('.pika-single')).get(index)));

    browser.sleep(1000);
  }

  selectTimeRangeApplyButton() {
    return element(by.css('app-time-range')).element(by.buttonText('APPLY')).click();
  }

  getChangesAlertTableTitle(previousText: string) {
    // browser.pause();
    let title = element(by.css('.col-form-label-lg'));
    return this.waitForTextChange(title, previousText).then(() => {
      return title.getText();
    });
  }

  getAlertStatusById(id: string) {
    return element(by.css('a[title="' + id + '"]'))
          .element(by.xpath('../..')).all(by.css('td a')).get(8).getText();
  }

  sortTable(colName: string) {
    element.all(by.css('table thead th')).all(by.linkText(colName)).get(0).click();
  }

  getCellValue(rowIndex: number, colIndex: number) {
    return element.all(by.css('table tbody tr')).get(rowIndex).all(by.css('td')).get(colIndex).getText();
  }

  expandMetaAlert(rowIndex: number) {
    element.all(by.css('table tbody tr')).get(rowIndex).element(by.css('.icon-cell.dropdown-cell')).click();
  }

  getHiddenRowCount() {
    return element.all(by.css('table tbody tr.d-none')).count();
  }

  getNonHiddenRowCount() {
    return element.all(by.css('table tbody tr:not(.d-none)')).count();
  }

  getAllRowsCount() {
    return element.all(by.css('table tbody tr')).count();
  }

  clickOnMetaAlertRow(rowIndex: number) {
    element.all(by.css('table tbody tr')).get(rowIndex).all(by.css('td')).get(5).click();
    browser.sleep(2000);
  }

  removeAlert(rowIndex: number) {
    return element.all(by.css('app-table-view .fa-chain-broken')).get(rowIndex).click();
  }

  loadSavedSearch(name: string) {
    element.all(by.css('app-saved-searches metron-collapse')).get(1).element(by.css('li[title="'+ name +'"]')).click();
    browser.sleep(1000);
  }

  loadRecentSearch(name: string) {
    element.all(by.css('app-saved-searches metron-collapse')).get(0).all(by.css('li')).get(2).click();
    browser.sleep(1000);
  }

  getTimeRangeButtonTextForNow() {
    return element.all(by.css('app-time-range button span')).getText();
  }

  getTimeRangeButtonAndSubText() {
    return waitForElementInVisibility(element(by.css('#time-range')))
    .then(() => element.all(by.css('app-time-range button span')).getText())
    .then(arr => {
        let retArr = [arr[0]];
        for (let i=1; i < arr.length; i++) {
          let dateStr = arr[i].split(' to ');
          let fromTime = new Date(dateStr[0]).getTime();
          let toTime = new Date(dateStr[1]).getTime();
          retArr.push((toTime - fromTime) + '');
        }
        return retArr;
    });
  }

  renameColumn(name: string, value: string) {
    element(by.cssContainingText('app-configure-table span', name))
    .element(by.xpath('../..'))
    .element(by.css('.input')).sendKeys(value);
  }

}
