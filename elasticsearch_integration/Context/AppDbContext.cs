using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace elasticsearch_integration.Context
{
    public class AppDbContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlServer("Data Source=.;Initial Catalog=Wander;Integrated Security=True;MultipleActiveResultSets=True");
        }
        public DbSet<Travel> Travels { get; set; }
    }

    public sealed class Travel
    {
        public int Id { get; set; }
        public string Title { get; set; }
        public string Description { get; set; }
    }
}
