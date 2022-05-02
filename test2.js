/**
 * Scheduler file to handle all the cron jobs running in the time interval
 */
const moment = require('moment');
const {
  repoModel,
  watchersModel,
  commitsModel,
  clonesModel,
  viewsModel,
  actionsModel,
  forksModel,
  releaseModel,
  licenseModel,
  languageModel,
  repoTreeModel,
  jobsModel,
  organizationsModel
} = require('../models/index');
const { filters, cards, onboardMetrics } = require('../api/index');
const { Logger } = require('../helpers/index');
const path = require('path');
const { environment } = require('../config/index');

/**
 * return error as object
 *
 * @async
 * @function returnErrorObject
 * @param {object} exc
 * @returns {object} - message
 * @author dev-team
 */

const returnErrorObject = exc => {
  try {
    if (exc.response) {
      return { message: exc.response.data?.message?.toUpperCase() };
    }
    return { message: exc.message.toUpperCase() };
  } catch (exception) {
    Logger.log('error', `Error in returnErrorObject in ${path.basename(__filename)}: ${JSON.stringify(exception)}`);
    throw exception;
  }
};

/**
 * fetch data from github
 *
 * @async
 * @function FetchDataFromApi
 * @param {object}
 * @returns {object} - response body and ratelimitremaining
 * @author dev-team
 */

const FetchDataFromApi = async ({ url, token, starFlag }) => {
  try {
    const {
      body,
      headers: { 'x-ratelimit-remaining': rateLimitRemaining }
    } = await onboardMetrics.ApiCall({ url, token, starFlag });
    return { body, rateLimitRemaining };
  } catch (exc) {
    Logger.log('error', `Error in FetchDataFromApi in ${path.basename(__filename)}: ${JSON.stringify(exc)}`);
    if (exc.response) {
      return { message: exc.response?.data?.message?.toUpperCase() };
    }
    return { message: exc?.message?.toUpperCase() };
  }
};

/**
 * Get and set watchers data
 *
 * @async
 * @function WatchersDataJob
 * @param {object} opts
 * @returns {string} - message
 * @author dev-team
 */

const WatchersDataJob = async opts => {
  try {
    let data = {};
    const { orgName, repoId, repoName } = { ...opts };
    const Today = new Date().toISOString().split('T')[0];
    const [getWatchersFromDb, [list, watchers]] = await Promise.all([watchersModel.GetWatchersFromDB(opts), cards.GetWatchersList(opts)]);
    if (getWatchersFromDb) {
      if (watchers && watchers.length) {
        data = getWatchersFromDb;
        let filteredData = data.list.filter(watch => watch.date.split('T')[0] !== Today);
        filteredData = filteredData.concat(list);
        data.list = filteredData;
        data.watchers = watchers;
        await watchersModel.SetWatchersToDB({ data, orgName });
      }
    } else {
      data = Object.assign({
        repo_id: repoId,
        repo_name: repoName,
        list,
        watchers
      });
      await watchersModel.SetWatchersToDB({ data, orgName });
    }
    return 'success';
  } catch (exc) {
    Logger.log('error', `Error in returnErrorObject in ${path.basename(__filename)}: ${JSON.stringify(exc)}`);
    throw exc;
  }
};

/**
 * Get and set releases data
 *
 * @async
 * @function ReleaseDataJob
 * @param {object} opts
 * @returns {string} - message
 * @author dev-team
 */

const ReleaseDataJob = async opts => {
  try {
    let data = {};
    const { orgName, repoId, repoName } = { ...opts };
    const Today = new Date().toISOString().split('T')[0];
    const [getReleaseFromDb, getReleaseList] = await Promise.all([releaseModel.GetReleaseFromDB(opts), cards.GetReleasesList(opts)]);
    if (getReleaseFromDb) {
      if (getReleaseList && getReleaseList.length) {
        data = getReleaseFromDb;
        let filteredData = data.list.filter(release => release.date.split('T')[0] !== Today);
        filteredData = filteredData.concat(getReleaseList);
        data.list = filteredData;
        await releaseModel.SetReleaseToDB({ data, orgName });
      }
    } else {
      data = Object.assign({
        repo_id: repoId,
        repo_name: repoName,
        list: getReleaseList
      });
      await releaseModel.SetReleaseToDB({ data, orgName });
    }
    return 'success';
  } catch (exc) {
    Logger.log('error', `Error in ReleaseDataJob in ${path.basename(__filename)}: ${JSON.stringify(exc)}`);
    return returnErrorObject(exc);
  }
};

/**
 * Get and set repo tree data
 *
 * @async
 * @function RepoTreeDataJob
 * @param {object} opts
 * @returns {string} - message
 * @author dev-team
 */

const RepoTreeDataJob = async opts => {
  try {
    let data = {};
    const { orgName, repoId, repoName, token } = { ...opts };
    const commitsDataFromDb = await commitsModel.GetCommitsFromDB(opts);
    if (commitsDataFromDb) {
      const [latestCommit] = commitsDataFromDb.list.sort((a, b) => new Date(b.date) - new Date(a.date));
      if (latestCommit && latestCommit.sha) {
        // const repoTree = await cards.GetRepoTree({ ...opts, sha: latestCommit.sha });
        const url = `https://${environment.githubHost}/repos/${orgName}/${repoName}/git/trees/${latestCommit.sha}`;
        const { body, message } = await FetchDataFromApi({ url, token });
        if (!message || message === 'NOT FOUND') {
          data = { repo_id: repoId, repo_name: repoName, list: body || [] };
          await repoTreeModel.SetRepoTreeToDB({ data, orgName });
          return true;
        }
        return { message };
      }
    }
    return false;
  } catch (exc) {
    Logger.log('error', `Error in RepoTreeDataJob in ${path.basename(__filename)}: ${JSON.stringify(exc)}`);
    return returnErrorObject(exc);
  }
};

/**
 * Get and set clones data
 *
 * @async
 * @function ClonesDataJob
 * @param {object} opts
 * @returns {string} - message
 * @author dev-team
 */

const ClonesDataJob = async opts => {
  try {
    let data = {};
    const { orgName, repoId, repoName } = { ...opts };
    const [getClonesFromDb, getClonesList] = await Promise.all([clonesModel.GetClonesFromDB(opts), cards.GetClonesList(opts)]);
    if (getClonesFromDb) {
      if (getClonesList && getClonesList.length) {
        data = getClonesFromDb;
        const sortedDbData = data.list.sort((a, b) => new Date(a.date) - new Date(b.date));
        const sortedApiData = getClonesList.sort((a, b) => new Date(a.date) - new Date(b.date));
        sortedDbData.pop();
        const filteredData = sortedDbData.concat(sortedApiData);
        data.list = [...new Map(filteredData.map(item => [item.date, item])).values()];
        await clonesModel.SetClonesToDB({ data, orgName });
      }
    } else {
      data = Object.assign({
        repo_id: repoId,
        repo_name: repoName,
        list: getClonesList
      });
      await clonesModel.SetClonesToDB({ data, orgName });
    }
    return 'success';
  } catch (exc) {
    Logger.log('error', `Error in ClonesDataJob in ${path.basename(__filename)}: ${JSON.stringify(exc)}`);
    return returnErrorObject(exc);
  }
};

/**
 * Get and set visitors and visits data
 *
 * @async
 * @function ViewsDataJob
 * @param {object} opts
 * @returns {string} - message
 * @author dev-team
 */

const ViewsDataJob = async opts => {
  try {
    let data = {};
    const { orgName, repoId, repoName } = { ...opts };
    const [getViewsFromDb, getViewsList] = await Promise.all([viewsModel.GetViewsFromDB(opts), cards.GetViewList(opts)]);
    if (getViewsFromDb) {
      if (getViewsList && getViewsList.length) {
        data = getViewsFromDb;
        const sortedDbData = data.list.sort((a, b) => new Date(a.date) - new Date(b.date));
        const sortedApiData = getViewsList.sort((a, b) => new Date(a.date) - new Date(b.date));
        sortedDbData.pop();
        const filteredData = sortedDbData.concat(sortedApiData);
        data.list = [...new Map(filteredData.map(item => [item.date, item])).values()];
        await viewsModel.SetViewsToDB({ data, orgName });
      }
    } else {
      data = Object.assign({
        repo_id: repoId,
        repo_name: repoName,
        list: getViewsList
      });
      await viewsModel.SetViewsToDB({ data, orgName });
    }
    return 'success';
  } catch (exc) {
    Logger.log('error', `Error in ViewsDataJob in ${path.basename(__filename)}: ${JSON.stringify(exc)}`);
    return returnErrorObject(exc);
  }
};

/**
 * Get and set actions data
 *
 * @async
 * @function ActionsDataJob
 * @param {object} opts
 * @returns {string} - message
 * @author dev-team
 */

const ActionsDataJob = async opts => {
  try {
    let data = {};
    const { orgName, repoId, repoName } = { ...opts };
    const [getActionsFromDb, getActionsList] = await Promise.all([actionsModel.GetActionsFromDB(opts), cards.GetActionsList(opts)]);
    if (getActionsFromDb && getActionsList && getActionsList.length) {
      data = getActionsFromDb;
      data.list = data.list.concat(getActionsList);
      data.list = Array.from(new Set(data.list.map(JSON.stringify))).map(JSON.parse);
    } else {
      data = Object.assign({
        repo_id: repoId,
        repo_name: repoName,
        list: getActionsList
      });
    }
    await actionsModel.SetActionsToDB({ data, orgName });
    return 'success';
  } catch (exc) {
    Logger.log('error', `Error in ActionsDataJob in ${path.basename(__filename)}: ${JSON.stringify(exc)}`);
    return returnErrorObject(exc);
  }
};

/**
 * Get and set forks data
 *
 * @async
 * @function ForksDataJob
 * @param {object} opts
 * @returns {string} - message
 * @author dev-team
 */

const ForksDataJob = async opts => {
  try {
    const { orgName, repoId, repoName } = { ...opts };
    const getForksList = await cards.GetForksList(opts);
    const data = Object.assign({
      repo_id: repoId,
      repo_name: repoName,
      list: getForksList
    });
    await forksModel.SetForksToDB({ data, orgName });
    return 'success';
  } catch (exc) {
    Logger.log('error', `Error in ForksDataJob in ${path.basename(__filename)}: ${JSON.stringify(exc)}`);
    return returnErrorObject(exc);
  }
};

/**
 * Get and set license data
 *
 * @async
 * @function LicenseDataJob
 * @param {object} opts
 * @returns {string} - message
 * @author dev-team
 */

const LicenseDataJob = async opts => {
  try {
    const { orgName, repoId, repoName } = { ...opts };
    const getLicenseObj = await cards.GetLicense(opts);
    const data = Object.assign({
      repo_id: repoId,
      repo_name: repoName,
      license: getLicenseObj
    });
    await licenseModel.SetLicenseToDB({ data, orgName });
    return 'success';
  } catch (exc) {
    Logger.log('error', `Error in LicenseDataJob in ${path.basename(__filename)}: ${JSON.stringify(exc)}`);
    return returnErrorObject(exc);
  }
};

/**
 * Get and set language data
 *
 * @async
 * @function LanguageDataJob
 * @param {object} opts
 * @returns {string} - message
 * @author dev-team
 */

const LanguageDataJob = async opts => {
  try {
    const { orgName, repoId, repoName } = { ...opts };
    const getLanguageObj = await cards.GetLanguages(opts);
    const data = Object.assign({
      repo_id: repoId,
      repo_name: repoName,
      language: getLanguageObj
    });
    await languageModel.SetLanguageToDB({ data, orgName });
    return 'success';
  } catch (exc) {
    Logger.log('error', `Error in LanguageDataJob in ${path.basename(__filename)}: ${JSON.stringify(exc)}`);
    return returnErrorObject(exc);
  }
};

/**
 * set next run
 *
 * @async
 * @function ScheduleDBJob
 * @param {number} installationId
 * @author dev-team
 */

const ScheduleDBJob = async installationId => {
  try {
    const expiresAt = moment()
      .add(1, 'hour')
      .utc()
      .format('YYYY-MM-DDTHH:mm:ss[Z]');
    const query = { installation_id: installationId, task: 'Metrics' };
    const update = { $set: { expires_at: expiresAt, task: 'Metrics', processing: false } };
    return await jobsModel.SubmitTask(query, update);
  } catch (exc) {
    Logger.log('error', `Error in ScheduleDBJob in ${path.basename(__filename)}: ${JSON.stringify(exc)}`);
    return returnErrorObject(exc);
  }
};

/**
 * primary job function
 *
 * @async
 * @function DailyScheduledJobs
 * @param {number} installationId
 * @returns {string} - message
 * @author dev-team
 */

const DailyScheduledJobs = async installationId => {
  Logger.log('info', '************ Daily Scheduler initiated ************');
  try {
    // get org info
    const {
      org_name: orgName,
      onboard_complete: onboardComplete,
      token,
      org_status: status,
      api_limit_exceeded: limit,
      api_limit_reached_count: reachedCount
    } = await organizationsModel.GetOrg({
      installation_id: installationId
    });
    if (!orgName && status.toUpperCase() !== 'ACTIVE') {
      Logger.log('error', `org not found or inactive with installation id ${installationId}`);
      return 'error';
    }
    // validate the on bording process completed or not
    if (!onboardComplete) {
      Logger.log('info', `On boarding jobs not fully completed for the ${orgName} organization`);
      return 'error';
    }
    // update metrics job processing flag as true
    await jobsModel.SubmitTask({ installation_id: installationId, task: 'Metrics' }, { $set: { processing: true } });

    if (!limit) {
      const opts = {
        orgName,
        token
      };
      // get latest repo details form github and update in db
      const orginalRepoList = await filters.GetReposList(orgName, token);
      await repoModel.UpdateMultiRepo(orginalRepoList, orgName);

      const repoList = await repoModel.GetRepoListFromDB({ onboard_complete: true, repo_enabled: true, deleted: false }, orgName);
      // eslint-disable-next-line no-restricted-syntax
      for await (const repo of repoList) {
        opts.repoId = repo.repo_id;
        opts.repoName = repo.repo_name;
        const ActionsJob = ActionsDataJob(opts);
        const WatchersJob = WatchersDataJob(opts);
        const ClonesJob = ClonesDataJob(opts);
        const ViewsJob = ViewsDataJob(opts);
        const ForksJob = ForksDataJob(opts);
        const ReleaseJob = ReleaseDataJob(opts);
        const LicenseJob = LicenseDataJob(opts);
        const LanguageJob = LanguageDataJob(opts);
        const RepoTreeJob = RepoTreeDataJob(opts);
        const metricsJobCollection = await Promise.allSettled([
          ActionsJob,
          WatchersJob,
          ClonesJob,
          ViewsJob,
          ForksJob,
          ReleaseJob,
          LicenseJob,
          LanguageJob,
          RepoTreeJob
        ]);

        // update api_limit_exceeded and api_limit_reached_count fields
        const limitReachFlag = metricsJobCollection.some(res => typeof res.value === 'object' && res.value.toUpperCase().includes('API LIMIT'));
        if (limitReachFlag) {
          await organizationsModel.UpdateOrg(
            { installation_id: installationId },
            { $set: { api_limit_exceeded: true, api_limit_reached_count: Number(reachedCount) + 1 } }
          );
          return 'error';
        }
      }
      await ScheduleDBJob(installationId);
    }
    Logger.log('info', '************ Daily Scheduler completed ************');
    return 'success';
  } catch (exc) {
    Logger.log('error', `Error in DailyScheduledJobs in ${path.basename(__filename)}: ${JSON.stringify(exc)}`);
    // update metrics job processing flag as false
    await jobsModel.SubmitTask({ installation_id: installationId, task: 'Metrics' }, { $set: { processing: false } });
    return 'error';
  }
};

module.exports = {
  DailyScheduledJobs
};
