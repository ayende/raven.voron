// -----------------------------------------------------------------------
//  <copyright file="StorageOptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Voron
{
	public class StorageOptions
	{
		public long LogFileSize { get; set; }

		public bool OwnsPagers { get; set; }

		public StorageOptions()
		{
			LogFileSize = 64*1024*1024;
			OwnsPagers = true;
		}
	}
}