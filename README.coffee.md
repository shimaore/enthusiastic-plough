What is it?
===========

A script to split the CDR database into smaller databases

How does it work?
=================

    run = (since,year) ->
      assert since?, 'since is required'
      assert year?, 'year is required'

It queries the CDR database in batches, and distributes each item into a target bin (database) for a given year.

      limit = cfg.limit ? 500

      console.log "#{pkg.name} #{pkg.version} starting for year #{year} at sequence #{since} for up to #{limit}."

      should_continue = true

      agent.get "#{cfg.source}/_changes"
      .accept 'json'
      .query
        limit: limit
        since: since
        include_docs: true
      .catch (error) ->
        console.log "get failed with #{error}"
        throw error
      .then ({body:{results}}) ->
        assert results?, "Missing results in #{JSON.stringify arguments}"
        should_continue = results.length > 0
        assert results.length > 0, 'No results.'
        console.log "Splitting #{results.length} results."
        savers = {}
        for change in results
          do (change) ->
            {seq,doc} = change
            since = seq
            assert seq?, 'Missing seq'
            assert doc?, 'Missing doc'
            target_month = doc?.variables?.start_stamp?.substr(0,7)

Note: `target_month` might also be absent for deleted records (`change.deleted is true`).

            if target_month? and target_month.substr(0,4) is year
              target = savers[target_month] ?= new Saver target_month
              target.push doc
            else
              console.log "Skipped #{doc._id}"

        observers = []
        for name,saver of savers
          do (saver) ->
            observers.push saver.flush()

        Promise.all observers
      .then ->
        agent.get since_url
        .accept 'json'
        .agent agent
      .catch (error) ->
        console.log "#{since_id}: #{error}"
        body:
          _id: since_id
      .then ({body:doc}) ->
        doc.since = since
        doc.year = year
        agent.put since_url
        .send doc
      .catch (error) ->
        console.log "#{since_id}: #{error}"
      .then ->
        run since, year if should_continue

    pkg = require './package.json'
    path = require 'path'
    config_file = path.join (path.dirname module.filename), 'config.json'
    cfg = require config_file
    Promise = require 'bluebird'
    PouchDB = require 'pouchdb'
    request = (require 'superagent-as-promised') require 'superagent'
    agent = request.agent ca:cfg.ca
    fs = Promise.promisifyAll require 'fs'
    assert = require 'assert'

    class SaverError extends Error

    class Saver
      constructor: (@name) ->
        assert @name?, 'Missing @name'
        @db = new PouchDB "#{cfg.targets}/cdrs-#{@name}",
          ajax:
            ca: cfg.ca
            timeout: cfg.timeout
        @queue = []

      push: (doc) ->
        assert doc?, 'Missing doc'
        delete doc._rev
        v = doc.variables
        doc._id = "#{v.start_stamp} #{v.ccnq_account} #{v.ccnq_from_e164} #{v.ccnq_to_e164} #{v.billsec}"
        @queue.push doc
        return

      flush: ->
        my_queue = @queue
        delete @queue
        console.log "#{@name}: Submitting #{my_queue.length} entries."
        @db.bulkDocs my_queue
        .then (responses) =>
          for response in responses when not response.ok
            throw new SaverError "Failed for #{response}"

    since_id = "#{pkg.name}.since"
    since_url = "#{cfg.source}/_local/#{since_id}"
    agent.get since_url
    .accept 'json'
    .catch (error) ->
      console.log "#{since_id}: #{error}"
      body:
        since: 1
        year: (new Date()).getFullYear().toString()
    .then ({body:{since,year}}) ->
      run since, year
    .catch (error) ->
      console.log "Stopped with #{error}"
      throw error
