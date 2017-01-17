﻿using MongoDB.Bson;
using System;
using System.Diagnostics;
using System.Web.Mvc;

namespace Hangfire.Mongo.Sample.Controllers
{
    public class HomeController : Controller
    {
        public ActionResult Index()
        {
            return View();
        }

        public ActionResult FireAndForget(ObjectId id)
        {
            for (int i = 0; i < 10; i++)
            {
                BackgroundJob.Enqueue(() => Debug.WriteLine("Hangfire fire-and-forget task started."));
            }

            return RedirectToAction("Index");
        }

        public ActionResult Delayed(ObjectId id)
        {
            for (int i = 0; i < 10; i++)
            {
                BackgroundJob.Schedule(() => Debug.WriteLine("Hangfire delayed task started!"), TimeSpan.FromMinutes(1));
            }

            return RedirectToAction("Index");
        }

        public ActionResult Recurring()
        {
            RecurringJob.AddOrUpdate(() => Debug.WriteLine("Hangfire recurring task started!"), Cron.Minutely);

            return RedirectToAction("Index");
        }
    }
}